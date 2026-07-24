import asyncio
import importlib
import pickle
import sys
import time

import pytest
from localqueue import DeliveryPolicy, DurabilityMode
from localqueue.bus import BaseEvent, BusTopology, EventBus, NoSubscribers


class UserCreated(BaseEvent):
    user_id: str


class OrderPlaced(BaseEvent):
    order_id: str


class RenamedEvent(BaseEvent):
    event_name = "custom-event"
    value: int


class PickleEnvelopeSerializer:
    def dumps(self, obj: dict[str, object], /) -> bytes:
        return pickle.dumps(obj)

    def loads(self, data: bytes, /) -> object:
        return pickle.loads(data)


@pytest.fixture
def bus(tmp_path):
    b = EventBus(
        str(tmp_path / "bus"),
        name="test",
        topology=BusTopology({"s1": ["*"]}),
        delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=1),
    )
    yield b
    b.close()


def run(coro):
    return asyncio.run(coro)


class TestImportGuard:
    def test_import_sem_pydantic_levanta_erro_claro(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "pydantic", None)
        for name in [n for n in sys.modules if n.startswith("localqueue.bus")]:
            monkeypatch.delitem(sys.modules, name)
        with pytest.raises(ImportError, match='pip install "localqueue\\[bus\\]"'):
            importlib.import_module("localqueue.bus")


class TestRegistration:
    def test_registro_direto_e_decorator(self, bus):
        calls = []

        def direct(event):
            calls.append("direct")

        bus.on(UserCreated, direct, subscription="s1")

        @bus.on(OrderPlaced, subscription="s1")
        def decorated(event):
            calls.append("decorated")

        assert ("s1", "UserCreated") in bus._handlers
        assert ("s1", "OrderPlaced") in bus._handlers

    def test_pattern_por_classe_string_e_wildcard(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology(
                {
                    "s1": [UserCreated],
                    "s2": [OrderPlaced],
                    "s3": ["*"],
                }
            ),
        )
        bus.on(UserCreated, lambda e: None, subscription="s1")
        bus.on("OrderPlaced", lambda e: None, subscription="s2")
        bus.on("*", lambda e: None, subscription="s3")

        assert bus._subscriptions_for("UserCreated") == ("s1", "s3")
        assert bus._subscriptions_for("OrderPlaced") == ("s2", "s3")
        bus.close()

    def test_registro_duplicado_rejeitado(self, bus):
        bus.on(UserCreated, lambda e: None, subscription="s1")
        with pytest.raises(ValueError, match="already registered"):
            bus.on(UserCreated, lambda e: None, subscription="s1")
        with pytest.raises(ValueError, match="already registered"):
            bus.on("UserCreated", lambda e: None, subscription="s1")

    def test_colisao_de_event_type_rejeitada(self, bus):
        bus.register(UserCreated)

        class Impostor(BaseEvent):
            event_name = "UserCreated"

        with pytest.raises(ValueError, match="already registered"):
            bus.on(Impostor, lambda e: None, subscription="s1")

    def test_override_de_nome_de_evento(self, bus):
        event = RenamedEvent(value=1)
        assert event.event_type == "custom-event"
        assert event.event_schema == "custom-event@1"

        bus.on(RenamedEvent, lambda e: None, subscription="s1")
        assert bus._subscriptions_for("custom-event") == ("s1",)

    def test_event_name_vazio_rejeitado(self):
        with pytest.raises(ValueError, match="event_name"):

            class Bad(BaseEvent):
                event_name = "  "

    @pytest.mark.parametrize("name", ["", "tem:dois-pontos", "-abc", "com espaço"])
    def test_nomes_invalidos(self, tmp_path, name):
        with pytest.raises(ValueError, match="invalid"):
            EventBus(str(tmp_path / "b"), name=name, topology=BusTopology({}))
        bus = EventBus(
            str(tmp_path / "ok"),
            name="ok",
            topology=BusTopology({"s1": [UserCreated]}),
        )
        with pytest.raises(ValueError):
            bus.on(UserCreated, lambda e: None, subscription=name)
        bus.close()


class TestDispatch:
    def test_dispatch_sem_subscribers_levanta(self, tmp_path):
        bus = EventBus(str(tmp_path / "bus"), topology=BusTopology({}))
        try:
            with pytest.raises(NoSubscribers):
                bus.dispatch(UserCreated(user_id="1"))
        finally:
            bus.close()

    def test_dispatch_sem_subscribers_permitido(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "bus"),
            name="t",
            topology=BusTopology({}),
            require_subscribers=False,
        )
        receipt = bus.dispatch(UserCreated(user_id="1"))
        assert receipt.subscriptions == ()
        assert receipt.message_ids == ()
        # Nada foi escrito em nenhuma fila.
        queue = bus._open_subscription_queue("qualquer")
        assert queue.stats()["ready"] == 0
        queue.close()
        bus.close()

    def test_fanout_uma_chamada_nativa_por_dispatch(self, tmp_path, monkeypatch):
        bus = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology(
                {"s1": [UserCreated], "s2": [UserCreated], "s3": ["*"]}
            ),
        )
        bus.on(UserCreated, lambda e: None, subscription="s1")
        bus.on(UserCreated, lambda e: None, subscription="s2")
        bus.on("*", lambda e: None, subscription="s3")

        calls = []
        native = bus._native_queue

        class SpyNative:
            def fanout(self, payload, targets):
                calls.append(targets)
                return native.fanout(payload, targets)

            def close(self):
                return native.close()

        monkeypatch.setattr(bus, "_native_queue", SpyNative())

        receipt = bus.dispatch(UserCreated(user_id="7"))
        assert len(calls) == 1
        assert len(calls[0]) == 3
        assert receipt.subscriptions == ("s1", "s2", "s3")
        assert len(receipt.message_ids) == 3
        bus.close()

    def test_dispatch_async(self, bus):
        bus.on(UserCreated, lambda e: None, subscription="s1")
        receipt = run(bus.dispatch_async(UserCreated(user_id="9")))
        assert receipt.subscriptions == ("s1",)


class TestConsumption:
    def test_handler_sync_e_async_ack(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology({"sync": [UserCreated], "async": [UserCreated]}),
            delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=1),
        )
        seen = []
        bus.on(
            UserCreated, lambda e: seen.append(("sync", e.user_id)), subscription="sync"
        )

        @bus.on(UserCreated, subscription="async")
        async def async_handler(event):
            await asyncio.sleep(0)
            seen.append(("async", event.user_id))

        bus.dispatch(UserCreated(user_id="42"))
        run(bus.run(idle_timeout=0.5))

        assert ("sync", "42") in seen
        assert ("async", "42") in seen
        for sub in ("sync", "async"):
            q = bus._open_subscription_queue(sub)
            assert q.stats()["acked"] == 1
            assert q.stats()["ready"] == 0
            q.close()
        bus.close()

    def test_uma_delivery_por_subscription_exato_vence_wildcard(self, bus):
        seen = []
        bus.on(UserCreated, lambda e: seen.append("exact"), subscription="s1")
        bus.on("*", lambda e: seen.append("wildcard"), subscription="s1")

        bus.dispatch(UserCreated(user_id="1"))
        run(bus.run_subscription("s1", idle_timeout=0.5))

        assert seen == ["exact"]
        q = bus._open_subscription_queue("s1")
        assert q.stats()["acked"] == 1
        q.close()

    def test_nack_e_retry_em_erro_transitorio(self, bus):
        attempts = []

        def flaky(event):
            attempts.append(1)
            if len(attempts) < 2:
                raise RuntimeError("transitório")

        bus.on(UserCreated, flaky, subscription="s1")
        bus.dispatch(UserCreated(user_id="1"))
        run(bus.run_subscription("s1", idle_timeout=0.5))

        assert len(attempts) == 2
        q = bus._open_subscription_queue("s1")
        assert q.stats()["acked"] == 1
        q.close()

    def test_fail_em_erro_permanente(self, bus):
        class FatalError(Exception):
            pass

        def doomed(event):
            raise FatalError("não adianta retentar")

        bus.on(UserCreated, doomed, subscription="s1", permanent_errors=(FatalError,))
        bus.dispatch(UserCreated(user_id="1"))
        run(bus.run_subscription("s1", idle_timeout=0.5))

        q = bus._open_subscription_queue("s1")
        assert q.stats()["failed"] == 1
        failed = q.list_failed()
        assert "não adianta retentar" in failed[0]["last_error"]
        q.close()

    def test_dead_letter_apos_estourar_retries(self, bus):
        def always_fails(event):
            raise RuntimeError("sempre falha")

        bus.on(UserCreated, always_fails, subscription="s1")
        bus.dispatch(UserCreated(user_id="1"))
        run(bus.run_subscription("s1", idle_timeout=0.5))

        # max_retries=1 -> 2 tentativas e dead-letter.
        q = bus._open_subscription_queue("s1")
        assert q.stats()["failed"] == 1
        assert q.stats()["ready"] == 0
        q.close()

    def test_reconstrucao_pydantic_do_payload(self, bus):
        seen = []

        class StrictTyped(BaseEvent):
            count: int

        bus.on(
            StrictTyped,
            lambda e: seen.append((e.count, type(e.count))),
            subscription="s1",
        )
        bus.dispatch(StrictTyped(count=5))
        run(bus.run_subscription("s1", idle_timeout=0.5))
        assert seen == [(5, int)]

    def test_payload_invalido_vai_para_dead_letter(self, bus):
        class StrictMissing(BaseEvent):
            count: int

        bus.register(StrictMissing)
        bus.on("StrictMissing", lambda e: None, subscription="s1")

        # Envelope com payload inválido (campo ausente) injetado direto na fila.
        queue = bus._open_subscription_queue("s1")
        queue.put(
            {
                "event_id": "12345678-1234-1234-1234-123456789012",
                "event_type": "StrictMissing",
                "event_schema": "StrictMissing@1",
                "event_created_at": "2026-01-01T00:00:00+00:00",
                "payload": {"outro": 1},
            }
        )
        queue.close()
        run(bus.run_subscription("s1", idle_timeout=0.5))

        q = bus._open_subscription_queue("s1")
        assert q.stats()["failed"] == 1
        assert "invalid payload" in q.list_failed()[0]["last_error"]
        q.close()

    def test_evento_desconhecido_vai_para_dead_letter(self, bus):
        bus.on("*", lambda e: None, subscription="s1")

        queue = bus._open_subscription_queue("s1")
        queue.put(
            {
                "event_id": "12345678-1234-1234-1234-123456789012",
                "event_type": "TipoInexistente",
                "event_schema": "TipoInexistente@1",
                "event_created_at": "2026-01-01T00:00:00+00:00",
                "payload": {},
            }
        )
        queue.close()
        run(bus.run_subscription("s1", idle_timeout=0.5))

        q = bus._open_subscription_queue("s1")
        assert q.stats()["failed"] == 1
        assert "unknown event" in q.list_failed()[0]["last_error"]
        q.close()

    def test_duas_subscriptions_recebem_o_mesmo_evento(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology({"sa": [UserCreated], "sb": [UserCreated]}),
            delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=1),
        )
        seen = []
        bus.on(UserCreated, lambda e: seen.append("a"), subscription="sa")
        bus.on(UserCreated, lambda e: seen.append("b"), subscription="sb")

        bus.dispatch(UserCreated(user_id="1"))
        run(bus.run(idle_timeout=0.5))
        assert sorted(seen) == ["a", "b"]
        bus.close()

    def test_persistencia_apos_crash(self, tmp_path):
        path = str(tmp_path / "bus")
        topology = BusTopology({"s1": [UserCreated]})
        bus = EventBus(
            path,
            name="test",
            topology=topology,
            delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=1),
        )
        bus.on(UserCreated, lambda e: None, subscription="s1")
        bus.dispatch(UserCreated(user_id="sobrevive"))
        bus.close()  # "crash" antes de consumir

        seen = []
        bus2 = EventBus(
            path,
            name="test",
            topology=topology,
            delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=1),
        )
        bus2.on(UserCreated, lambda e: seen.append(e.user_id), subscription="s1")
        run(bus2.run_subscription("s1", idle_timeout=0.5))
        bus2.close()
        assert seen == ["sobrevive"]

    def test_dedup_mesmo_event_id(self, bus):
        event = UserCreated(user_id="1")
        bus.on(UserCreated, lambda e: None, subscription="s1")
        bus.dispatch(event)
        bus.dispatch(event)  # redispatch do mesmo evento
        q = bus._open_subscription_queue("s1")
        assert q.stats()["ready"] == 1
        q.close()

    def test_run_cancelamento_limpo(self, bus):
        bus.on(UserCreated, lambda e: None, subscription="s1")

        async def main():
            task = asyncio.create_task(bus.run())
            await asyncio.sleep(0.3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        run(main())
        # Bus continua utilizável após cancelamento.
        receipt = bus.dispatch(UserCreated(user_id="pós-cancel"))
        assert receipt.subscriptions == ("s1",)


class GroupEvent(BaseEvent):
    seq: int


def _consumer_group_worker(
    path, processed, active, concurrent_duplicates, lock, idle_timeout
):
    """Worker de consumer group em processo separado."""
    bus = EventBus(
        path,
        name="test",
        topology=BusTopology({"x": [GroupEvent]}),
        delivery=DeliveryPolicy(lease_seconds=5.0, max_retries=1),
    )

    def handle(event):
        import os

        event_id = str(event.event_id)
        process_id = os.getpid()
        with lock:
            if event_id in active:
                concurrent_duplicates.append((event_id, active[event_id], process_id))
            active[event_id] = process_id
        time.sleep(0.02)
        with lock:
            processed.append((event_id, process_id))
            active.pop(event_id, None)

    bus.on(GroupEvent, handle, subscription="x")
    try:
        asyncio.run(bus.run_subscription("x", idle_timeout=idle_timeout))
    finally:
        bus.close()


class TestValidations:
    def test_constructor_rejeita_delivery_invalida(self, tmp_path):
        with pytest.raises(TypeError, match="delivery.*DeliveryPolicy"):
            EventBus(
                str(tmp_path / "b"),
                name="t",
                topology=BusTopology({}),
                delivery=object(),
            )

    @pytest.mark.parametrize(
        "permanent_errors",
        ["NaoETupla", (str,), [Exception, 42]],
    )
    def test_permanent_errors_invalido_rejeitado(self, bus, permanent_errors):
        with pytest.raises(TypeError, match="permanent_errors"):
            bus.on(
                UserCreated,
                lambda e: None,
                subscription="s1",
                permanent_errors=permanent_errors,
            )

    def test_durability_repassada_para_subscription_queue(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "b"),
            name="t",
            topology=BusTopology({"s1": ["*"]}),
            durability=DurabilityMode.DURABLE,
        )
        queue = bus._open_subscription_queue("s1")
        _, synchronous = queue._native.pragma_settings()
        queue.close()
        bus.close()
        assert synchronous == 2  # FULL


class TestEnvelopeMalformed:
    def test_envelope_lista_crua_vai_para_dead_letter(self, bus):
        bus.on("*", lambda e: None, subscription="s1")
        queue = bus._open_subscription_queue("s1")
        queue.put(["não", "é", "um", "envelope"])
        queue.close()

        run(bus.run_subscription("s1", idle_timeout=0.5))

        q = bus._open_subscription_queue("s1")
        assert q.stats()["failed"] == 1
        assert "malformed envelope" in q.list_failed()[0]["last_error"]
        q.close()

    def test_envelope_sem_chaves_obrigatorias(self, bus):
        bus.on("*", lambda e: None, subscription="s1")
        queue = bus._open_subscription_queue("s1")
        queue.put({"payload": {"user_id": "1"}})  # sem event_type
        queue.put({"event_type": "UserCreated"})  # sem payload
        queue.close()

        run(bus.run_subscription("s1", idle_timeout=0.5))

        q = bus._open_subscription_queue("s1")
        assert q.stats()["failed"] == 2
        errors = [item["last_error"] for item in q.list_failed()]
        assert errors == [
            "malformed envelope: missing or invalid 'event_type'",
            "malformed envelope: missing or invalid 'payload'",
        ]
        q.close()

    @pytest.mark.parametrize(
        ("envelope", "expected_error"),
        [
            (
                ["not", "an", "object"],
                "malformed envelope: expected a JSON object, got list",
            ),
            (
                {"event_type": 42, "payload": {}},
                "malformed envelope: missing or invalid 'event_type'",
            ),
            (
                {"event_type": "UserCreated", "payload": []},
                "malformed envelope: missing or invalid 'payload'",
            ),
        ],
    )
    def test_minimum_envelope_errors_fail_with_stable_messages(
        self, bus, envelope, expected_error
    ):
        bus.on("*", lambda event: None, subscription="s1")
        queue = bus._open_subscription_queue("s1")
        queue.put(envelope)
        queue.close()

        run(bus.run_subscription("s1", idle_timeout=0.2))

        failed = bus._open_subscription_queue("s1")
        try:
            assert failed.stats()["failed"] == 1
            assert failed.list_failed()[0]["last_error"] == expected_error
        finally:
            failed.close()

    @pytest.mark.parametrize(
        "envelope",
        [
            {
                "event_type": "UserCreated",
                "event_created_at": "2026-01-01T00:00:00+00:00",
                "payload": {"user_id": "1"},
            },
            {
                "event_id": "12345678-1234-1234-1234-123456789012",
                "event_type": "UserCreated",
                "payload": {"user_id": "1"},
            },
        ],
    )
    def test_later_envelope_errors_keep_invalid_payload_classification(
        self, bus, envelope
    ):
        bus.on(UserCreated, lambda event: None, subscription="s1")
        queue = bus._open_subscription_queue("s1")
        queue.put(envelope)
        queue.close()

        run(bus.run_subscription("s1", idle_timeout=0.2))

        failed = bus._open_subscription_queue("s1")
        try:
            assert failed.stats()["failed"] == 1
            error = failed.list_failed()[0]["last_error"]
            assert error.startswith("invalid payload for 'UserCreated':")
        finally:
            failed.close()

    def test_non_string_payload_key_keeps_invalid_payload_classification(
        self, tmp_path
    ):
        bus = EventBus(
            str(tmp_path / "bus"),
            name="test",
            topology=BusTopology({"s1": ["*"]}),
            delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=1),
            serializer=PickleEnvelopeSerializer(),
        )
        bus.on(UserCreated, lambda event: None, subscription="s1")
        queue = bus._open_subscription_queue("s1")
        queue.put(
            {
                "event_id": "12345678-1234-1234-1234-123456789012",
                "event_type": "UserCreated",
                "event_created_at": "2026-01-01T00:00:00+00:00",
                "payload": {1: "not a string key"},
            }
        )
        queue.close()

        try:
            run(bus.run_subscription("s1", idle_timeout=0.2))
            failed = bus._open_subscription_queue("s1")
            try:
                assert failed.stats()["failed"] == 1
                error = failed.list_failed()[0]["last_error"]
                assert error.startswith("invalid payload for 'UserCreated':")
                assert "keywords must be strings" in error
            finally:
                failed.close()
        finally:
            bus.close()

    def test_missing_process_local_handler_fails_the_delivery(self, bus):
        bus.on(UserCreated, lambda event: None, subscription="s1")
        bus.on(OrderPlaced, lambda event: None, subscription="s1")
        bus.dispatch(UserCreated(user_id="1"))
        bus._handlers.pop(("s1", "UserCreated"))

        run(bus.run_subscription("s1", idle_timeout=0.2))

        failed = bus._open_subscription_queue("s1")
        try:
            assert failed.stats()["failed"] == 1
            assert failed.list_failed()[0]["last_error"] == (
                "no handler registered for 'UserCreated' in 's1' in this process"
            )
        finally:
            failed.close()


class TestTypedReconstruction:
    def test_dispatch_com_somente_wildcard_reconstroi_evento(self, bus):
        seen = []
        bus.on("*", lambda e: seen.append(e), subscription="s1")

        bus.dispatch(GroupEvent(seq=7))
        run(bus.run_subscription("s1", idle_timeout=0.5))

        assert len(seen) == 1
        assert isinstance(seen[0], GroupEvent)
        assert seen[0].seq == 7

    def test_dispatch_com_handler_somente_string_reconstroi_evento(self, bus):
        seen = []
        bus.on("GroupEvent", lambda e: seen.append(e), subscription="s1")

        bus.dispatch(GroupEvent(seq=8))
        run(bus.run_subscription("s1", idle_timeout=0.5))

        assert len(seen) == 1
        assert isinstance(seen[0], GroupEvent)


class TestAsyncioSafety:
    def test_subscription_ociosa_faz_uma_sondagem_por_intervalo(self, bus, monkeypatch):
        from localqueue.core import SimpleQueue

        calls = []

        def empty_get(self, block=True, timeout=None):
            calls.append((block, timeout, time.monotonic()))
            from localqueue import Empty

            raise Empty("fila vazia")

        monkeypatch.setattr(SimpleQueue, "get", empty_get)
        bus.on(UserCreated, lambda event: None, subscription="s1")

        run(bus.run_subscription("s1", idle_timeout=0.25))

        assert 2 <= len(calls) <= 4
        call_arguments = {(block, timeout) for block, timeout, _ in calls}
        assert call_arguments == {(False, None)}
        intervals = [
            current[2] - previous[2] for previous, current in zip(calls, calls[1:])
        ]
        assert all(interval >= 0.08 for interval in intervals)

    def test_event_loop_nao_bloqueia_durante_get_e_handler_sync(self, bus):
        import threading

        handler_started = threading.Event()
        release_handler = threading.Event()
        released_by_event_loop = []

        def slow_handler(event):
            handler_started.set()
            released_by_event_loop.append(release_handler.wait(timeout=1.0))

        bus.on(UserCreated, slow_handler, subscription="s1")
        bus.dispatch(UserCreated(user_id="1"))

        async def main():
            task = asyncio.create_task(bus.run(idle_timeout=0.5))
            assert await asyncio.to_thread(handler_started.wait, 2.0)
            release_handler.set()
            await task

        run(main())
        assert released_by_event_loop == [True]

    def test_event_loop_nao_bloqueia_durante_ack(self, bus, monkeypatch):
        import threading

        from localqueue.core import SimpleQueue

        ack_started = threading.Event()
        release_ack = threading.Event()
        released_by_event_loop = []
        original_ack = SimpleQueue.ack

        def slow_ack(self, job):
            ack_started.set()
            released_by_event_loop.append(release_ack.wait(timeout=1.0))
            return original_ack(self, job)

        monkeypatch.setattr(SimpleQueue, "ack", slow_ack)
        bus.on(UserCreated, lambda event: None, subscription="s1")
        bus.dispatch(UserCreated(user_id="1"))

        async def main():
            task = asyncio.create_task(bus.run(idle_timeout=0.5))
            assert await asyncio.to_thread(ack_started.wait, 2.0)
            release_ack.set()
            await task

        run(main())
        assert released_by_event_loop == [True]


class TestLeaseSafety:
    def test_handler_longo_com_heartbeat_faz_ack(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "b"),
            name="t",
            topology=BusTopology({"s1": [UserCreated]}),
            delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=1),
        )
        seen = []

        def long_handler(event):
            time.sleep(1.2)  # mais que o dobro do lease
            seen.append(event.user_id)

        bus.on(UserCreated, long_handler, subscription="s1")
        bus.dispatch(UserCreated(user_id="longo"))
        run(bus.run_subscription("s1", idle_timeout=0.5))
        bus.close()

        assert seen == ["longo"]
        bus2 = EventBus(
            str(tmp_path / "b"),
            name="t",
            topology=BusTopology({"s1": [UserCreated]}),
            delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=1),
        )
        q = bus2._open_subscription_queue("s1")
        assert q.stats()["acked"] == 1
        assert q.stats()["failed"] == 0
        q.close()
        bus2.close()

    def test_lease_expired_no_ack_nao_mata_consumer(self, bus, monkeypatch):
        from localqueue.core import SimpleQueue

        seen = []
        bus.on(UserCreated, lambda e: seen.append(e.user_id), subscription="s1")
        bus.dispatch(UserCreated(user_id="a"))
        bus.dispatch(UserCreated(user_id="b"))

        original_ack = SimpleQueue.ack
        failed_once = []

        def ack_que_expira(self, job):
            if not failed_once:
                failed_once.append(job.id)
                from localqueue import LeaseExpired

                raise LeaseExpired("lease expirado simulado")
            return original_ack(self, job)

        monkeypatch.setattr(SimpleQueue, "ack", ack_que_expira)
        run(bus.run_subscription("s1", idle_timeout=0.5))

        # O consumer sobreviveu: os dois handlers rodaram.
        assert "a" in seen and "b" in seen

    def test_lease_perdido_descarta_resultado(self, bus, monkeypatch):
        from localqueue import LeaseExpired
        from localqueue.core import SimpleQueue

        seen = []

        def handler(event):
            # Dorme o suficiente para o heartbeat (lease/3) disparar.
            time.sleep(0.4)
            seen.append(event.user_id)

        bus.on(UserCreated, handler, subscription="s1")
        bus.dispatch(UserCreated(user_id="x"))

        def extend_que_falha(self, job, seconds):
            raise LeaseExpired("lease perdido simulado")

        monkeypatch.setattr(SimpleQueue, "extend_lease", extend_que_falha)
        run(bus.run_subscription("s1", idle_timeout=0.5))

        # Handler rodou (possivelmente reentregue, pois o lease expira de
        # verdade), mas o ack foi sempre descartado por lease_lost.
        assert "x" in seen
        q = bus._open_subscription_queue("s1")
        assert q.stats()["acked"] == 0
        q.close()


class TestConsumerGroup:
    def test_dois_processos_competem_sem_duplicar(self, tmp_path):
        import multiprocessing

        path = str(tmp_path / "group")
        num_events = 40
        expected_event_ids = []

        topology = BusTopology({"x": [GroupEvent]})
        bus = EventBus(
            path,
            name="test",
            topology=topology,
            delivery=DeliveryPolicy(lease_seconds=5.0, max_retries=1),
        )
        for i in range(num_events):
            event = GroupEvent(seq=i)
            expected_event_ids.append(str(event.event_id))
            bus.dispatch(event)
        bus.close()

        context = multiprocessing.get_context("spawn")
        with context.Manager() as manager:
            processed = manager.list()
            active = manager.dict()
            concurrent_duplicates = manager.list()
            lock = manager.Lock()
            processes = [
                context.Process(
                    target=_consumer_group_worker,
                    args=(
                        path,
                        processed,
                        active,
                        concurrent_duplicates,
                        lock,
                        2.0,
                    ),
                )
                for _ in range(2)
            ]
            try:
                for process in processes:
                    process.start()
                for process in processes:
                    process.join(timeout=60)

                assert [process.exitcode for process in processes] == [0, 0]
                assert list(concurrent_duplicates) == []
                processed_event_ids = [event_id for event_id, _ in processed]
                assert sorted(processed_event_ids) == sorted(expected_event_ids)
                assert len(processed_event_ids) == num_events
            finally:
                for process in processes:
                    if process.is_alive():
                        process.terminate()
                    process.join(timeout=5)

        verification_bus = EventBus(path, name="test", topology=topology)
        queue = verification_bus._open_subscription_queue("x")
        try:
            stats = queue.stats()
            assert stats["acked"] == num_events
            assert stats["ready"] == 0
            assert stats["processing"] == 0
            assert stats["failed"] == 0
        finally:
            queue.close()
            verification_bus.close()
