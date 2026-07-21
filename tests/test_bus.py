import asyncio
import importlib
import sys

import pytest

from localqueue.bus import BaseEvent, EventBus, NoSubscribers


class UserCreated(BaseEvent):
    user_id: str


class OrderPlaced(BaseEvent):
    order_id: str


class RenamedEvent(BaseEvent):
    event_name = "custom-event"
    value: int


@pytest.fixture
def bus(tmp_path):
    b = EventBus(str(tmp_path / "bus"), name="test", lease_seconds=0.5, max_retries=1)
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

        @bus.on(UserCreated, subscription="s2")
        def decorated(event):
            calls.append("decorated")

        assert ("s1", "UserCreated") in bus._handlers
        assert ("s2", "UserCreated") in bus._handlers

    def test_pattern_por_classe_string_e_wildcard(self, bus):
        bus.on(UserCreated, lambda e: None, subscription="s1")
        bus.on("OrderPlaced", lambda e: None, subscription="s2")
        bus.on("*", lambda e: None, subscription="s3")

        assert bus._subscriptions_for("UserCreated") == ("s1", "s3")
        assert bus._subscriptions_for("OrderPlaced") == ("s2", "s3")

    def test_registro_duplicado_rejeitado(self, bus):
        bus.on(UserCreated, lambda e: None, subscription="s1")
        with pytest.raises(ValueError, match="já registrado"):
            bus.on(UserCreated, lambda e: None, subscription="s1")
        with pytest.raises(ValueError, match="já registrado"):
            bus.on("UserCreated", lambda e: None, subscription="s1")

    def test_colisao_de_event_type_rejeitada(self, bus):
        bus.register(UserCreated)

        class Impostor(BaseEvent):
            event_name = "UserCreated"

        with pytest.raises(ValueError, match="já registrado"):
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
        with pytest.raises(ValueError, match="inválido"):
            EventBus(str(tmp_path / "b"), name=name)
        bus = EventBus(str(tmp_path / "ok"), name="ok")
        with pytest.raises(ValueError, match="inválido"):
            bus.on(UserCreated, lambda e: None, subscription=name)
        bus.close()


class TestDispatch:
    def test_dispatch_sem_subscribers_levanta(self, bus):
        with pytest.raises(NoSubscribers):
            bus.dispatch(UserCreated(user_id="1"))

    def test_dispatch_sem_subscribers_permitido(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "bus"), name="t", require_subscribers=False
        )
        receipt = bus.dispatch(UserCreated(user_id="1"))
        assert receipt.subscriptions == ()
        assert receipt.message_ids == ()
        # Nada foi escrito em nenhuma fila.
        queue = bus._open_subscription_queue("qualquer")
        assert queue.stats()["ready"] == 0
        queue.close()
        bus.close()

    def test_fanout_uma_chamada_nativa_por_dispatch(self, bus, monkeypatch):
        bus.on(UserCreated, lambda e: None, subscription="s1")
        bus.on(UserCreated, lambda e: None, subscription="s2")
        bus.on("*", lambda e: None, subscription="s3")

        calls = []
        native = bus._native_queue

        class SpyNative:
            def fanout(self, payload, targets):
                calls.append(targets)
                return native.fanout(payload, targets)

        monkeypatch.setattr(bus, "_native_queue", SpyNative())

        receipt = bus.dispatch(UserCreated(user_id="7"))
        assert len(calls) == 1
        assert len(calls[0]) == 3
        assert receipt.subscriptions == ("s1", "s2", "s3")
        assert len(receipt.message_ids) == 3

    def test_dispatch_async(self, bus):
        bus.on(UserCreated, lambda e: None, subscription="s1")
        receipt = run(bus.dispatch_async(UserCreated(user_id="9")))
        assert receipt.subscriptions == ("s1",)


class TestConsumption:
    def test_handler_sync_e_async_ack(self, bus):
        seen = []
        bus.on(UserCreated, lambda e: seen.append(("sync", e.user_id)),
               subscription="sync")

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

        bus.on(UserCreated, doomed, subscription="s1",
               permanent_errors=(FatalError,))
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

        bus.on(StrictTyped, lambda e: seen.append((e.count, type(e.count))),
               subscription="s1")
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
        assert "payload inválido" in q.list_failed()[0]["last_error"]
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
        assert "evento desconhecido" in q.list_failed()[0]["last_error"]
        q.close()

    def test_duas_subscriptions_recebem_o_mesmo_evento(self, bus):
        seen = []
        bus.on(UserCreated, lambda e: seen.append("a"), subscription="sa")
        bus.on(UserCreated, lambda e: seen.append("b"), subscription="sb")

        bus.dispatch(UserCreated(user_id="1"))
        run(bus.run(idle_timeout=0.5))
        assert sorted(seen) == ["a", "b"]

    def test_persistencia_apos_crash(self, tmp_path):
        path = str(tmp_path / "bus")
        bus = EventBus(path, name="test", lease_seconds=0.5, max_retries=1)
        bus.on(UserCreated, lambda e: None, subscription="s1")
        bus.dispatch(UserCreated(user_id="sobrevive"))
        bus.close()  # "crash" antes de consumir

        seen = []
        bus2 = EventBus(path, name="test", lease_seconds=0.5, max_retries=1)
        bus2.on(UserCreated, lambda e: seen.append(e.user_id),
                subscription="s1")
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
