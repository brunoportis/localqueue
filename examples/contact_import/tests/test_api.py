from concurrent.futures import ThreadPoolExecutor

from api import app
from fastapi.testclient import TestClient


def test_creates_a_contact_and_rejects_a_duplicate(tmp_path, monkeypatch):
    monkeypatch.setenv("CONTACTS_DB", str(tmp_path / "contacts.lmdb"))

    with TestClient(app) as client:
        payload = {
            "cnpj": "12345678000190",
            "nome": "Ana Silva",
            "banco": "Banco do Brasil",
            "conta": "12345678",
            "agencia": "1234",
        }

        created = client.post("/contacts", json=payload)
        duplicate = client.post("/contacts", json=payload)

    assert created.status_code == 201
    assert created.json()["cnpj"] == payload["cnpj"]
    assert duplicate.status_code == 409


def test_rejects_an_invalid_cnpj(tmp_path, monkeypatch):
    monkeypatch.setenv("CONTACTS_DB", str(tmp_path / "contacts.lmdb"))

    with TestClient(app) as client:
        response = client.post(
            "/contacts",
            json={
                "cnpj": "invalid",
                "nome": "Ana Silva",
                "banco": "Banco do Brasil",
                "conta": "12345678",
                "agencia": "1234",
            },
        )

    assert response.status_code == 422


def test_replays_the_same_idempotency_key_and_rejects_another_key(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("CONTACTS_DB", str(tmp_path / "contacts.lmdb"))
    payload = {
        "cnpj": "12345678000190",
        "nome": "Ana Silva",
        "banco": "Bank",
        "conta": "12345678",
        "agencia": "1234",
    }

    with TestClient(app) as client:
        created = client.post(
            "/contacts", json=payload, headers={"Idempotency-Key": "one"}
        )
        replay = client.post(
            "/contacts", json=payload, headers={"Idempotency-Key": "one"}
        )
        conflict = client.post(
            "/contacts", json=payload, headers={"Idempotency-Key": "two"}
        )

    assert created.status_code == 201
    assert replay.status_code == 200
    assert conflict.status_code == 409


def test_accepts_concurrent_contact_writes(tmp_path, monkeypatch):
    monkeypatch.setenv("CONTACTS_DB", str(tmp_path / "contacts.lmdb"))

    with TestClient(app) as client:

        def create(index: int) -> int:
            response = client.post(
                "/contacts",
                json={
                    "cnpj": f"1234567800{index:04d}",
                    "nome": f"Contact {index}",
                    "banco": "Bank",
                    "conta": "12345678",
                    "agencia": "1234",
                },
            )
            return response.status_code

        with ThreadPoolExecutor(max_workers=20) as executor:
            statuses = list(executor.map(create, range(20)))

    assert statuses == [201] * 20
