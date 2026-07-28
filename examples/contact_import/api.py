"""Minimal contact API used by the CSV import example."""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock
from typing import AsyncIterator

import lmdb
from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

write_lock = Lock()


class Contact(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    cnpj: str = Field(pattern=r"^\d{14}$")
    nome: str = Field(min_length=1, max_length=200)
    banco: str = Field(min_length=1, max_length=200)
    conta: str = Field(min_length=1, max_length=50)
    agencia: str = Field(min_length=1, max_length=50)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    database_path = Path(os.environ.get("CONTACTS_DB", "data/contacts.lmdb"))
    database_path.parent.mkdir(parents=True, exist_ok=True)
    app.state.contacts = lmdb.open(str(database_path), map_size=10 * 1024 * 1024)
    try:
        yield
    finally:
        app.state.contacts.close()


app = FastAPI(title="Contact import demo API", lifespan=lifespan)


@app.post("/contacts", response_model=Contact, status_code=status.HTTP_201_CREATED)
def create_contact(
    contact: Contact,
    request: Request,
    idempotency_key: str | None = Header(default=None),
) -> Contact | JSONResponse:
    """Store a contact once, using CNPJ as its duplicate key."""
    key = b"contact:" + contact.cnpj.encode()
    payload = json.dumps(contact.model_dump()).encode()
    environment: lmdb.Environment = request.app.state.contacts

    with write_lock:
        with environment.begin(write=True) as transaction:
            if idempotency_key is not None:
                replay = transaction.get(b"idempotency:" + idempotency_key.encode())
                if replay is not None:
                    if replay != payload:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="idempotency key reused with a different contact",
                        )
                    return JSONResponse(
                        content=json.loads(replay), status_code=status.HTTP_200_OK
                    )
            if transaction.get(key) is not None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT, detail="duplicate_cnpj"
                )
            transaction.put(key, payload)
            if idempotency_key is not None:
                transaction.put(b"idempotency:" + idempotency_key.encode(), payload)

    return contact
