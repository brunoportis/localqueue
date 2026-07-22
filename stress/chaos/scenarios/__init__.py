"""Isolated operational scenarios executed against the public queue API."""

from .backup_restore import run as backup_restore
from .corruption import run as corruption
from .disk_full import run as disk_full
from .durability import full as synchronous_full
from .durability import normal as synchronous_normal
from .locking import run as lock_timeout
from .process_crash import maintenance_termination, producer_termination, wal_recovery
from .readonly import run as readonly

SCENARIOS = {
    "disk-full": disk_full,
    "readonly": readonly,
    "lock-timeout": lock_timeout,
    "wal-recovery": wal_recovery,
    "corruption": corruption,
    "synchronous-normal": synchronous_normal,
    "synchronous-full": synchronous_full,
    "producer-termination": producer_termination,
    "maintenance-termination": maintenance_termination,
    "backup-restore": backup_restore,
}
