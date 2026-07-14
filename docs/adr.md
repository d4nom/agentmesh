# Architecture Decision Records

Short records of the decisions that shaped this platform. All were fixed by
the brief up front; these capture the reasoning, not a debate.

## ADR-001: NATS JetStream as the bus, not Kafka or HTTP

**Context.** Agents need to communicate without knowing about each other,
with at-least-once delivery, durable queuing, and a DLQ path — and the brief
explicitly forbids HTTP between agents.

**Decision.** NATS JetStream.

**Rationale.**
- Kafka gives similar durability guarantees but at far higher operational
  weight (ZooKeeper/KRaft, partition planning, JVM tuning) for a system this
  size — massive overkill for a handful of agent roles and workqueue-style
  task distribution.
- Plain HTTP between agents would mean every agent needs to know the
  network address of its downstream agents, turning topology into a
  service-discovery problem and losing durability (a dead receiver drops
  the request) — exactly what the brief's "one agent's failure must not
  block the system" requirement rules out.
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
