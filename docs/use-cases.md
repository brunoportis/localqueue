---
icon: lucide/briefcase
---

# Use Cases

`localqueue` shines when the work can stay on one machine and the value is in
durable retries, local recovery, and simple terminal operations.

The pages in this section focus on workflows that are already well supported by
the current queue, worker, dead-letter, and persistent-retry APIs.

By default, `localqueue` stores queue and retry state under the usual XDG data
location for your user.

## The three strongest workflows

### Local outbox

Accept work now, run the external side effect later on the same machine.

This is the best fit when the caller should return quickly and the worker can
consume the job from a shared local store.

[Read the local outbox guide](use-cases/local-outbox.md)

### Operator recovery

Inspect failed jobs, review dead-letter records, and replay them from the
terminal after the underlying issue is fixed.

This is the best fit when a human operator needs a local recovery loop instead
of ad hoc shell scripts.

[Read the operator recovery guide](use-cases/operator-recovery.md)

### Persistent retries

Keep retry budgets across restarts when another part of the system already
delivers the work.

This is the best fit when you need durable attempt tracking but not a queue
lifecycle.

[Read the persistent retries guide](use-cases/persistent-retries.md)
