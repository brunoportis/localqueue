"""Events for the resumable customer import example.

``CustomerCreationRequested`` opts into durable identity scoped to the import
operation: the pair ``(import_id, external_id)`` identifies one logical
customer creation, so re-ingesting the same row (same identity, same payload)
is deduplicated instead of delivered twice. ``CustomerCreated`` deliberately
has no identity: each occurrence is an audit record of one creation and no
duplicate scenario requires deduplication for it.
"""

from __future__ import annotations

from localqueue.bus import BaseEvent, event


@event(identity=("import_id", "external_id"))
class CustomerCreationRequested(BaseEvent):
    """Request to create one customer as part of one bulk import."""

    event_name = "customer.creation-requested"

    import_id: str
    external_id: str
    name: str
    email: str
    phone: str


class CustomerCreated(BaseEvent):
    """Audit record emitted when a customer exists after a creation request."""

    event_name = "customer.created"

    import_id: str
    external_id: str
    customer_id: str
