# 1. Public Go SDK

Status: Accepted

## Context

Every Go consumer of the hub among sock's own apps builds on an internal client library, which carries the transport, retries and error text, but not the messaging loops. Each consumer wrote its own: the Subscribe reconnect loop with an idle watchdog exists five times, Send-or-NotifyOwner with an idempotency key four times, the fetch-and-ack drain three times, and the copies disagree (which refusals end a stream, whether Retry-After is honored). That library also carries what only sock's own apps need: puppet and project resolution, the CLI config file, Apple sign-in through a browser.

## Decision

`github.com/aeryx-ai/sock-go-sdk` is the public Go SDK. One package, `sock`, holds what any client of the hub needs: the authenticated Connect client, the retrier, error classification and text, and one version each of Listen, Drain, Send, NotifyOwner and Ack.

Puppet and project resolution, config files, sign-in flows and terminal output stay out: they are decisions of a particular app, not of the API.

The SDK depends on the published sock-contract module for its generated types, which are part of its API. It never vendors a copy.

The consumers move onto it later, one at a time, deleting their own copies; this repo changes none of them.

## Consequences

- One reconnect policy: stop on a rejected token, a suspension or a caller class that may not subscribe; retry other refusals a minute apart; otherwise 1s to 5s jittered, never sooner than Retry-After (capped at an hour).
- Listen hands over messages only. A consumer that needs read markers or activity events (the desktop app) needs Listen to grow an event hook before it can move.
- The SDK cannot be fetched outside the org until sock-contract is published. Publishing it is the prerequisite for releasing this module.
- Error text names no command or environment variable of any one app.

## Alternatives

- Grow the internal client library into the SDK in place: its import path and app-specific surface would be the public API.
- Vendor the generated stubs into the SDK: a second copy of the contract that drifts from the tag.
