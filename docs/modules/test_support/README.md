# `test_support`

Opt-in helpers for downstream module integration tests, enabled with the
`test-support` feature.

## Artifact admission

`artifact_path` computes Cargo's platform library name. `admit_module` reads
the selected path from the caller's environment variable, checks the adjacent
`modules.toml` digest, and requires a private directory containing only that
library before calling `ModuleHost::load_dir`.

TinyBus intentionally does not unload a mapped module. Admission therefore
allows one loader attempt per process: preflight failures leave the slot free,
but after the loader boundary the slot stays consumed even if the loader
returns an error or the admitted name differs from the expected name. Tests
that need independent dynamic modules must run in separate processes.

## Broker helpers

`start_bus` starts an in-memory broker and returns its host, client and task.
`wait_until_serving` and `wait_until_idle` use caller-provided deadlines, and
`call` forwards a typed member invocation through a proxy. The helpers do not
start sockets, download module artifacts, or hide a timeout behind a fixed
sleep.
