# Shared sandbox types

This package owns `RuntimeMetadata`, `SandboxType`, and `EgressClass`, including
identity logging and the empty-type fallback to a regular sandbox. It contains
no worker resources, network initialization, or VM lifecycle management.

Callers import these types directly. `SandboxType.EgressClass()` selects the
traffic class; DSCP configuration and socket handling belong to worker networking.
The resource-owning `Sandbox` stays in the orchestrator package. Runtime team
metadata is attribution, not trusted authorization data.
