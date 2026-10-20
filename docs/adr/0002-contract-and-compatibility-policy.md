# ADR 0002: Contract and compatibility policy

- Status: Accepted
- Date: 2026-10-20

## Decision

All internal service contracts live under `harnessx.v1`. Backward-compatible
fields may be added within a major version; field numbers and enum values are
never reused. Breaking changes require a new protobuf package version.

The northbound HTTP API may provide compatibility surfaces such as OpenAI-style
responses. Compatibility formats terminate at `api-gateway`. Internal model
contracts remain provider-neutral and typed.

Service-provider implementations expose capabilities explicitly. Callers must
not infer support from provider names or hard-coded tool names.

Errors crossing a service boundary use canonical gRPC status codes and may
include a stable machine-readable reason. Retriability is explicit; callers do
not retry arbitrary failures.

Generated protobuf sources are committed so downstream users and build systems
do not require a generator toolchain merely to compile the repository.
