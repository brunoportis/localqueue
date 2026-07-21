import pytest

from localqueue import Empty, EnqueueItem, SimpleQueue


@pytest.fixture
def queue(tmp_path):
    path = tmp_path / "batch"
    q = SimpleQueue(str(path), lease_seconds=0.5, max_retries=2)
    yield q
    q.close()


class TestPutMany:
    def test_put_many_vazio_retorna_lista_vazia(self, queue):
        assert queue.put_many([]) == []
        assert queue.stats()["ready"] == 0

    def test_put_many_varios_itens_mesma_fila(self, queue):
        ids = queue.put_many([{"i": i} for i in range(5)])
        assert len(ids) == 5
        assert queue.stats()["ready"] == 5

        for i in range(5):
            job = queue.get(block=False)
            assert job.data == {"i": i}
            queue.ack(job)
        with pytest.raises(Empty):
            queue.get(block=False)

    def test_put_many_ids_na_ordem_de_entrada(self, queue):
        ids = queue.put_many([{"i": i} for i in range(10)])
        assert ids == sorted(ids)

    def test_put_many_com_enqueue_item(self, queue):
        ids = queue.put_many(
            [
                EnqueueItem(data={"task": "a"}, job_id="job-1"),
                {"task": "b"},
                EnqueueItem(data={"task": "c"}),
            ]
        )
        assert len(ids) == 3
        assert queue.stats()["ready"] == 3

    def test_put_many_dedup_por_job_id(self, queue):
        first = queue.put_many([EnqueueItem(data={"v": 1}, job_id="job-1")])
        second = queue.put_many(
            [EnqueueItem(data={"v": 2}, job_id="job-1")]
        )

        assert second == first
        assert queue.stats()["ready"] == 1

        # Payload original é preservado.
        job = queue.get(block=False)
        assert job.data == {"v": 1}
        queue.ack(job)

    def test_put_many_mesmo_job_id_repetido_no_mesmo_batch(self, queue):
        ids = queue.put_many(
            [
                EnqueueItem(data={"v": 1}, job_id="job-1"),
                EnqueueItem(data={"v": 2}, job_id="job-1"),
                EnqueueItem(data={"v": 3}, job_id="job-2"),
            ]
        )
        assert ids[0] == ids[1]
        assert ids[2] != ids[0]
        assert queue.stats()["ready"] == 2

    def test_put_many_erro_de_serializacao_nao_escreve_nada(self, queue):
        # A serialização acontece antes da chamada nativa: se algum item
        # falha, nenhuma escrita parcial ocorre.
        with pytest.raises(TypeError):
            queue.put_many([{"ok": 1}, {"bad": object()}])
        assert queue.stats()["ready"] == 0

    def test_put_many_job_ids_tamanho_invalido(self, queue):
        with pytest.raises(ValueError, match="job_ids"):
            queue._native.put_many([b"a", b"b"], [None])
        assert queue.stats()["ready"] == 0

    def test_put_continua_funcionando(self, queue):
        job_id = queue.put({"task": "one"}, job_id="j-1")
        same = queue.put({"task": "dup"}, job_id="j-1")
        assert same == job_id
        job = queue.get(block=False)
        assert job.data == {"task": "one"}
        queue.ack(job)


class TestFanout:
    def test_fanout_mesma_mensagem_em_filas_diferentes(self, tmp_path):
        path = tmp_path / "fanout"
        source = SimpleQueue(str(path), name="source", lease_seconds=5.0)
        qa = SimpleQueue(str(path), name="queue-a", lease_seconds=5.0)
        qb = SimpleQueue(str(path), name="queue-b", lease_seconds=5.0)
        try:
            payload = source.serializer.dumps({"event": "created"})
            ids = source._native.fanout(
                payload, [("queue-a", None), ("queue-b", None)]
            )
            assert len(ids) == 2

            for target in (qa, qb):
                job = target.get(block=False)
                assert job.data == {"event": "created"}
                target.ack(job)

            # A fila de origem não recebe nada.
            with pytest.raises(Empty):
                source.get(block=False)
        finally:
            source.close()
            qa.close()
            qb.close()

    def test_fanout_mesmo_job_id_em_filas_diferentes(self, tmp_path):
        path = tmp_path / "fanout-dedup"
        source = SimpleQueue(str(path), name="source", lease_seconds=5.0)
        qa = SimpleQueue(str(path), name="queue-a", lease_seconds=5.0)
        qb = SimpleQueue(str(path), name="queue-b", lease_seconds=5.0)
        try:
            payload = source.serializer.dumps({"event": 1})
            ids = source._native.fanout(
                payload,
                [("queue-a", "evt-1"), ("queue-b", "evt-1")],
            )
            assert ids[0] != ids[1]
            assert qa.stats()["ready"] == 1
            assert qb.stats()["ready"] == 1

            # Repetir o fanout deduplica por fila sem duplicar.
            ids2 = source._native.fanout(
                payload,
                [("queue-a", "evt-1"), ("queue-b", "evt-1")],
            )
            assert ids2 == ids
            assert qa.stats()["ready"] == 1
            assert qb.stats()["ready"] == 1
        finally:
            source.close()
            qa.close()
            qb.close()
