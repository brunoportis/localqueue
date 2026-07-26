"""Shared routing topology for the customer import example.

Both the producer and the worker import this module; the producer never
imports worker handlers. Routing stays explicit and shared so dispatch and
consumption always agree on subscription names.
"""

from __future__ import annotations

from localqueue.bus import BusTopology

from examples.resumable_customer_import.events import (
    CustomerCreated,
    CustomerCreationRequested,
)

#: Logical bus name shared by producer and worker. Ingestion checkpoints are
#: scoped to the bus name, so both sides must use the same one.
BUS_NAME = "customer-import"

CUSTOMER_CREATOR = "customer-creator"
CUSTOMER_AUDIT = "customer-audit"

TOPOLOGY = BusTopology(
    {
        CUSTOMER_CREATOR: [CustomerCreationRequested],
        CUSTOMER_AUDIT: [CustomerCreated],
    }
)
