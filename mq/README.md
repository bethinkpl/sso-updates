# User update broadcasts

sso and user-data tell the other services (today: every wnl-platform instance and e-commerce) about changes to a user.
Both import this package from `github.com/bethinkpl/sso-updates/mq`, so they always publish the same contract.

## Transport

GCP Pub/Sub: one topic (`PUBSUB_TOPIC_ID`), one subscription per consumer, **ordering key = user ID**.

| Variable | Meaning |
|---|---|
| `PUBSUB_PROJECT_ID` | required, GCP project of the topic, e.g. `bethink-prod` |
| `PUBSUB_TOPIC_ID` | required, the topic: `sso-updates-events` |
| `PUBSUB_ENDPOINT` | optional, `europe-west1-pubsub.googleapis.com:443` by default: messages are kept in order only when they are published in one region |
| `PUBSUB_EMULATOR_HOST` | local development only, e.g. `pubsub:8085` |

A broadcast succeeds only once Pub/Sub has stored the message, otherwise the request fails with HTTP 500.
The publisher only needs the `roles/pubsub.publisher` role on the topic; it never looks the topic up. Without
credentials (and without an emulator) the service refuses to start.

Every message is about a single user. Messages are delivered **at least once**: consumers must cope with duplicates.
`mq_broadcasts_total{broker, event_type, result}` counts the broadcasts.

## Body

The body is the JSON below, unchanged since the first broadcasts. **Its shape must not change**: the consumers
tell the messages apart by the keys that are present. Every field marked as optional is left out when empty.

| `event_type` | Sent by | Body |
|---|---|---|
| `email_updated` | sso, `PATCH /users/:id` with `email` and/or `email_verified_at` | `{"id":1,"email":"a@b.pl","email_verified_at":"2019-09-22 16:49:39"}`, both fields optional |
| `force_logout` | sso, `PATCH /users/:id` with `force_logout` (sent after `email_updated` when both apply) | `{"id":1,"force_logout":true,"cutoff_date":"2019-09-22 16:49:39"}`, `force_logout` is always `true`, `cutoff_date` (the request's `updated_at`) optional |
| `personal_data_updated` | user-data, `PATCH /users/:id` | `{"id":1,"first_name":"Zenon","last_name":"Kowalski"}`, both names optional |
| `delete_user` | user-data, `DELETE /users/:id` | `{"id":1,"type":"delete_user"}` |

`id` is the SSO user ID, shared by both services.

## Metadata

Sent next to the body (Pub/Sub attributes) and never inside it.

| Name | Example | Meaning |
|---|---|---|
| `event_id` | `0192d3a4-5b6c-7d8e-9f01-23456789abcd` | UUIDv7, unique per event - use it to drop duplicates |
| `event_type` | `email_updated` | see the table above |
| `source` | `sso` | `sso` or `user-data` |
| `occurred_at` | `2026-10-29T10:00:00.123Z` | when the event was created, RFC 3339 in UTC with milliseconds |
| `schema_version` | `1` | version of this metadata |
| `user_id` | `123456` | the same as the body's `id` |

The **ordering key** of an event is the user ID in decimal (`"123456"`), the same in both services
(`testdata/ordering_keys.json`): a subscription with message ordering enabled delivers the events of one user
in the order they were published.

A retried HTTP request creates a new event, with a new `event_id`.

## Local Pub/Sub emulator

The local names follow wnl-platform: project `local-project`, topic `sso-updates-events`. Its
`sso:process-updates-queue` worker creates the topic and an ordered subscription on the emulator by itself, so the
setup below is only needed without the platform stack.

```sh
docker run -d --name pubsub -p 8085:8085 gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085

# the topic and an ordered subscription to read the broadcasts from
curl -X PUT localhost:8085/v1/projects/local-project/topics/sso-updates-events
curl -X PUT localhost:8085/v1/projects/local-project/subscriptions/sso-updates-local \
  -H 'Content-Type: application/json' \
  -d '{"topic": "projects/local-project/topics/sso-updates-events", "enableMessageOrdering": true}'

# run the service with
# PUBSUB_EMULATOR_HOST=localhost:8085 PUBSUB_PROJECT_ID=local-project PUBSUB_TOPIC_ID=sso-updates-events

# read what was broadcast
curl -X POST localhost:8085/v1/projects/local-project/subscriptions/sso-updates-local:pull \
  -H 'Content-Type: application/json' -d '{"maxMessages": 10}'
```
