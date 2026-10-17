# sock-go-sdk

Public Go SDK for the sock hub. Module `github.com/aeryx-ai/sock-go-sdk`, package `sock`.

- Scope: what any hub client needs. Puppet/project resolution, config files, sign-in flows and terminal output belong to the apps, not here.
- Error text and docs name no app's commands or environment variables.
- Generated types come from the tagged sock-contract module in go.mod. Never vendor or `replace` it: a public module cannot carry a replace.
- `make check` is the gate; CI runs the same target.
- Never push; commit locally.
