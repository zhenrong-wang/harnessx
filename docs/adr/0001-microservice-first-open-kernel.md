# ADR 0001: Microservice-first open kernel

- Status: Accepted
- Date: 2026-10-20

## Context

HarnessX is a server-side harness for durable model-driven execution. It must
support multiple model providers, server-side tools, filesystem-like durable
volumes, context engineering, streaming, and auditability without absorbing
every product feature that can be built from those capabilities.

Introducing process boundaries after a monolithic implementation has matured
would require mechanical extraction, data ownership changes, failure-semantics
redesign, and contract stabilization at the same time. HarnessX instead treats
the service boundaries as part of its public architecture from the beginning.

## Decision

HarnessX begins as a monorepo containing independently runnable services. The
services communicate through versioned protobuf/gRPC contracts:

1. `api-gateway`: public HTTP/SSE transport and edge concerns.
2. `harness-core`: authoritative run and step state machine.
3. `model-core`: normalized model execution, adapters, routing, and fallback.
4. `context-core`: provider-neutral context construction and reduction.
5. `tool-core`: server-side tool discovery and execution.
6. `volume-core`: durable filesystem-like storage.
7. `event-core`: ordered events, audit append, subscription, and replay.

The orchestration kernel uses the vocabulary `Run`, `Step`, `Action`,
`Observation`, `ArtifactRef`, `PolicySnapshot`, and `Event`. Product-specific
features must compose through service contracts and must not add hidden state
transitions to `harness-core`.

One Go module is used initially for coherent dependency management. Service
packages do not import other services' implementation packages. Generated
contracts and infrastructure-only packages may be shared.

One PostgreSQL instance may be used in development and small deployments, with
service-owned schemas and migrations. A service must never query another
service's schema.

## Core invariants

- Only `harness-core` advances run state.
- A model proposes output or actions; it never executes a tool directly.
- Every accepted transition is durably recorded.
- Events are ordered within a run and safe to replay.
- A resolved policy snapshot is immutable for a run.
- Deadlines, cancellation, request IDs, trace IDs, and idempotency propagate
  across service calls.
- Provider-native payloads may be retained for diagnostics but are not kernel
  contracts.
- Plugins advertise versioned capabilities and return normalized errors.

## Outside the kernel

Document ingestion, knowledge bases, memory products, search implementations,
browser and desktop runtimes, skill marketplaces, identity, organizations,
billing, and specialized event projections are integrations. They may ship in
the ecosystem without becoming privileged orchestration behavior.

## Consequences

The project accepts distributed-systems concerns from day one: partial failure,
timeouts, retries, idempotency, schema compatibility, and observability. In
return, service and plugin boundaries do not need to be extracted after product
behavior has accumulated around internal implementation details.
