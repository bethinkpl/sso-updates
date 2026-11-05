SSO updates
======================

The broadcasts of user changes sent by [sso](https://github.com/bethinkpl/sso) and
[user-data](https://github.com/bethinkpl/user-data) to the services that keep their own copy of the user data
(every wnl-platform instance and e-commerce): email updates, forced logouts, personal data updates and deletions.

The messages, their metadata, the configuration and the local Pub/Sub emulator are described in
[`mq/README.md`](mq/README.md). **The message bodies are a contract with the consumers: their shape must not change.**

## Usage

```
go get github.com/bethinkpl/sso-updates@<version>
```

```go
import "github.com/bethinkpl/sso-updates/mq"

// once, before serving requests: PUBSUB_* env vars
if err := mq.Init(ctx, mq.ConfigFromEnv("sso", log)); err != nil { ... }
defer mq.Close(ctx)

// in a handler
err := mq.Broadcast(ctx, mq.EmailUpdated, userID, payload)

// expose mq_broadcasts_total next to the other metrics
mq.RegisterMetrics(prometheus.DefaultRegisterer)
```

In tests, `mq.SetForTest(t, &mq.Recorder{})` records the broadcasts instead of sending them.

## Developing

`make test` runs the tests (Pub/Sub is faked in-process, nothing else is needed), `make lint` runs `golangci-lint`.

Releases are git tags (`vX.Y.Z`): sso and user-data pick them up with `go get github.com/bethinkpl/sso-updates@vX.Y.Z`.
Change the message bodies or attributes only together with the consumers.
