"""Resumable bulk customer import example.

Demonstrates a durable, resumable CSV import pipeline built only with the
public localqueue APIs: ``CsvSource``, ``EventBus.ingest`` with a checkpoint,
durable event identity, typed handler contexts, and explicit Retry/Reject
control flow.
"""
