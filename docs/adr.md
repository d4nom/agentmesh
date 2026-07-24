# Architecture Decision Records

Short records of the decisions that shaped this platform in response to the
requirements and the PoC's operating constraints. They capture the reasoning
and tradeoffs.

## ADR-001: NATS JetStream as the bus, not Kafka or HTTP

**Context.** Agents need to communicate without knowing about each other,
with durable queuing, retry/DLQ semantics, and failure isolation. The brief
does not forbid HTTP; avoiding direct agent-to-agent HTTP is an architectural
choice made to meet those requirements.

**Decision.** NATS JetStream.

**Rationale.**
- Kafka gives similar durability guarantees but at far higher operational
  weight (ZooKeeper/KRaft, partition planning, JVM tuning) for a system this
  size — massive overkill for a handful of agent roles and workqueue-style
  task distribution.
- Plain HTTP between agents would mean every agent needs to know the
  network address of its downstream agents, turning topology into a
  service-discovery problem and losing durability (a dead receiver drops
  an unpersisted request). That works against the requirement that one
  agent's failure must not block the rest of the system.
- NATS JetStream gives durable streams, pull consumers, ack/nak,
  `max_deliver`, and per-message redelivery out of the box, with a single
  lightweight binary and a Python client that's easy to wrap in a small SDK.
  Subject-based routing (`tasks.<role>`) is also a natural fit for
  YAML-declared topology — an agent's whole wiring is "what subject do I
  subscribe to, what do I publish to."

**Consequences.** No request/reply-with-timeout semantics as clean as HTTP
would give (mitigated by `reply_to` + `correlation_id` in the Envelope, if a
future agent needs it). No cross-datacenter fan-out story as mature as
Kafka's — not a concern at this scale.

## ADR-002: No central orchestrator

**Context.** Something has to decide what happens after `parser` finishes:
does it call `rag` next, or does `rag` just know to listen?

**Decision.** No orchestrator process. Each agent subscribes to exactly one
subject and publishes to whatever subjects its logic calls for; the YAML
config only *declares* those subjects, it doesn't execute a workflow.

**Rationale.** An orchestrator becomes a single point of failure and a
hidden coupling point — every agent's contract with the world starts routing
through one process that has to know the whole graph. It also directly
conflicts with the acceptance criterion that one agent's failure can't block
the system: if the orchestrator is down, nothing moves, no matter how
healthy the agents are. Routing-via-subject means the bus itself is the only
shared infrastructure, and it's already required to be highly available.

**Consequences.** Topology is implicit in the union of all agents'
`subscribes`/`publishes` — there's no single diagram-generating source of
truth beyond reading the YAML (mitigated: the YAML is small and the mermaid
diagram in the README documents the shipped topology explicitly). Adding a
new agent to a pipeline means editing that agent's own YAML entry and
whichever upstream agent needs to publish to it — for the summarizer
example, that upstream edit is one line changing `executor`'s `publishes`,
still with zero code change.

## ADR-003: No agent framework (LangChain, LangGraph, CrewAI, AutoGen)

**Context.** These frameworks would provide agent abstractions, tool-calling,
and orchestration primitives for free.

**Decision.** Build the SDK from scratch on nats-py, pydantic, httpx, and
OpenTelemetry directly.

**Rationale.** The brief's premise is that this is infrastructure — a
platform other teams build agents *on top of*, not a demo of an agent
framework's capabilities. Bringing in LangChain-class frameworks would mean
the actual load-bearing logic (message durability, retries, idempotency,
tracing) lives inside a third-party framework's assumptions about execution
model, which is precisely the thing this platform needs to own and control
(e.g. exact `nak`/backoff/DLQ semantics tied to JetStream's consumer model,
`traceparent` propagation matching the Envelope contract exactly). It also
keeps the dependency surface small and auditable, and keeps "agent" meaning
exactly what it means here: an async Python class with a `handle()` method,
nothing more.

**Consequences.** No built-in tool-calling/agent-loop abstractions — any
agent that wants an LLM ReAct-style loop has to build it on top of
`LLMProvider` itself. That's an acceptable tradeoff for a platform whose job
is reliable message routing, not reasoning loops.

## ADR-004: At-least-once delivery, not an exactly-once claim

**Context.** JetStream redelivers unacknowledged messages. A process can die
after publishing a downstream result but before recording completion and
acking the input.

**Decision.** Prefer a possible duplicate over silent loss. The SDK checks a
Redis completion marker scoped by durable consumer before `handle()`, writes it
only after the handler and downstream publish succeed, and then acks. It extends
the JetStream ack lease across the Redis check, handler, marker write and
settlement. A lost final ack leaves the marker in place, so redelivery skips the
handler and retries only settlement. Retries and DLQ behavior remain available
because a failed first attempt is never marked as complete.

**Consequences.** Redelivery of the same already-marked `message_id` is
suppressed for that consumer, while another legitimate event subscriber keeps
an independent marker. There is still a crash window between the downstream
side effect and the completion marker. The platform therefore promises
at-least-once delivery with best-effort duplicate suppression, not exactly-once
processing. A production agent with destructive effects must add a domain
idempotency key or a transactional outbox supported by its target system.

## ADR-005: Work-queue tasks and fan-out events are different contracts

**Context.** A generic subject bus can accidentally imply that every subject
supports both competing work and broadcast delivery. JetStream retention
semantics make that ambiguity dangerous.

**Decision.** `tasks.>` uses `WORK_QUEUE` retention: one task subject denotes
one competing-consumer role. Branching work is published to distinct task
subjects. `events.>` uses limits retention and may have multiple independent
durable consumers. Durable names and Redis markers include agent and subscribed
subject identity; the display-level system name is deliberately excluded so a
compatible YAML topology change reconnects to the same work-queue consumer.

**Consequences.** Two task roles cannot overlap on the same `tasks.foo` filter;
that constraint is deliberate and documented. Event observers do not suppress
each other through a global idempotency key. Reusing an existing durable with a
different filter is rejected at startup instead of silently binding new code to
old consumer state. Because display-level `system` is not part of this identity,
independent deployments sharing a NATS account and Redis database need external
namespace/isolation.

## ADR-006: Liveness depends on the bus, observability does not

**Context.** A heartbeat-only task can leave a green process whose consumer has
already died. Conversely, making Jaeger a hard startup dependency can take down
healthy business processing solely because the trace UI is unavailable.

**Decision.** Supervise both consumer and bus-heartbeat tasks. Update the
health marker only after a successful NATS heartbeat; tolerate brief failures,
then exit after a bounded consecutive-failure threshold so the container
restart policy can recover the service. Do not gate agent startup on Jaeger.
Bound stored events to seven days/256 MiB and dead letters to 30 days/512 MiB.

**Consequences.** A NATS outage eventually makes an agent unhealthy and restarts
it, while a Jaeger outage only loses/export-delays telemetry. Compose still
proves single-node PoC recovery, not clustered infrastructure HA.

## ADR-007: DLQ handoff owns the retry ceiling

**Context.** If the broker's consumer `max_deliver` equals the application's
five-attempt poison-message threshold, a failure to publish the DLQ record on
the fifth delivery can strand the original message: it is neither terminated
nor eligible for another server delivery.

**Decision.** Configure agent consumers with server-side
`max_deliver=-1` (unlimited) and keep five as the SDK's application threshold.
From the fifth delivery onward, the SDK repeatedly attempts
publish-to-DLQ-then-`term()`. It terminates the source only after JetStream has
acknowledged the DLQ publication. Existing durable consumers have this runtime
policy and `ack_wait` reconciled at startup.

**Consequences.** A permanently unavailable DLQ does not silently lose the
source message; it remains pending and the supervised agent may restart until
infrastructure recovers. An unavailable DLQ can therefore cause repeated
delivery attempts beyond five, which is intentional: preservation is preferred
to an unrecorded drop.
