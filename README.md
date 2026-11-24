# Captain Hooks

[![CI](https://github.com/andymond/captain-hooks/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/andymond/captain-hooks/actions/workflows/ci.yml)

A Kotlin/JVM library for receiving signed webhooks, deduplicating deliveries, and running handlers in the background. Use the CLI to scaffold a handler or embed it in Spring MVC, Spring Boot, or Ktor.

## Quick start

With JDK 17+ and `kotlinc` available, run:

```sh
bash webhook test
bash webhook generate my_vendor
WEBHOOK_SECRET=demo-secret bash webhook serve my_vendor
```

The generated handler lives at `src/webhooks/my_vendor/Handler.kt` and listens at `POST http://127.0.0.1:8080/webhooks/my_vendor`. Vendor names start with a letter and use letters, digits, and underscores. The CLI lowercases the name for both the package and URL. The generated handler runs synchronously, uses local files for deduplication, and needs no database. The launcher can also find IntelliJ IDEA’s bundled compiler on macOS. Use `KOTLINC` to point to another compiler and `WEBHOOK_COROUTINES_JAR` if that distribution omits the coroutine JAR.

## Processing modes

| Mode | How it runs | Example |
| --- | --- | --- |
| `SYNC` | Handles the verified body during the request. Enable `persistWebhooks` to archive it first. | Generated handlers |
| `ASYNC_ARCHIVE` | Saves the original body and queues a reference. Requires `persistWebhooks = true` and a readable archive. | `incident_alerts` |
| `ASYNC_PROJECTED` | Queues only the bytes returned by the handler’s `project` function. Raw archiving is optional. | `shipment_updates` |

## Design notes

Delivery is at least once. Async requests return `204` after durable enqueue, and a duplicate of an accepted job returns `204` without starting a new job. Storage failures return `503` so the sender can retry. Worker leases prevent simultaneous claims during the lease. A crash after an external side effect can still cause a retry, so use the delivery ID as an idempotency key.

Sync mode suits short handlers when the sender should see handling failures directly. Archive mode keeps the original bytes for later handling and replay at the cost of storage and retention. Projected mode keeps the queue payload small and can avoid retaining sensitive fields. Its projection runs on the request path and must retain everything the worker will need.

Archive and queue adapters can use different databases. The archive write comes first. If enqueue fails, the original stays available and a retry can safely reuse it.

## Choose storage

Choose the archive and job queue independently. Applications supply and own their clients.

| Backend | Archive | Job queue | Setup |
| --- | --- | --- | --- |
| JDBC | `JdbcWebhookArchive(dataSource, dialect)` | `JdbcWebhookJobs(dataSource, dialect)` | PostgreSQL has live tests. MySQL, MariaDB, H2, and SQLite are experimental. Supply a JDBC driver and `JdbcDialect`, then call `createSchema()` or apply equivalent migrations. |
| Redis | `RedisWebhookArchive(redis)` | `RedisWebhookJobs(redis)` | Supply an application-managed Jedis client. Configure persistence and protect the keys from eviction. |
| MongoDB | `MongoWebhookArchive(collection)` | `MongoWebhookJobs(collection)` | Use separate collections with acknowledged writes. Call `createIndexes()` on the queue during setup. |

Archives keep the first body for each vendor and delivery ID. The endpoint saves the archive before enqueueing. If the enqueue fails, the sender can retry and reuse that record. Retain archived bodies while jobs still reference them.

### Run an async demo

The CLI uses JDBC for both storage roles. Set `WEBHOOK_DATABASE_URL` and `WEBHOOK_JDBC_JAR`, plus `WEBHOOK_DATABASE_USER` and `WEBHOOK_DATABASE_PASSWORD` if required. The CLI selects the dialect from the URL and initializes its tables. The [SQL scripts](db/) show the PostgreSQL schema for applications that manage it separately.

```sh
WEBHOOK_SECRET=demo-secret bash webhook serve incident_alerts
```

In another terminal, send a signed request. Run it twice with the same ID to see duplicate acknowledgment.

```sh
id=alert-1
body='Checkout is down'
signature=$(printf '%s\n%s' "$id" "$body" |
  openssl dgst -sha256 -hmac demo-secret | awk '{print $NF}')

curl -i --data-binary "$body" \
  -H "X-Webhook-Delivery: $id" \
  -H "X-Webhook-Signature: sha256=$signature" \
  http://127.0.0.1:8080/webhooks/incident_alerts
```

The signature covers the delivery ID, a newline byte, and the raw body. Use a stable ID containing 1–128 ASCII letters, digits, or characters from `._:-`.

## Embed in Spring or Ktor

Run `mvn install` from the repository root to install the modules locally. Gradle consumers can add `mavenLocal()`. All artifacts use group `io.github.andymond` and version `0.1.0-SNAPSHOT`.

| Artifact | Include when |
| --- | --- |
| `captain-hooks` | You use the core API with local receipts or application-supplied adapters. |
| `captain-hooks-jdbc` | You want the JDBC archive or queue. Supply your database driver. Example SQL is packaged with this artifact. |
| `captain-hooks-redis` | You want the Redis archive or queue. This artifact brings in Jedis. |
| `captain-hooks-mongo` | You want the MongoDB archive or queue. This artifact brings in the MongoDB Java driver. |

Each backend artifact brings in the core automatically. Include multiple adapters when your archive and queue use different backends.

Create a `WebhookEndpoint(handler, secret, receipts = receipts, archive = archive, jobs = jobs)` with the storage your handler needs. For synchronous handling, pass a `DeliveryReceipts` implementation backed by your infrastructure or use `FileDeliveryReceipts(path)`. Your receipt adapter claims each vendor and delivery ID atomically and marks completion only after the handler succeeds. Async handlers use their queue for deduplication. For async handlers, start `endpoint.startWorker()` at application startup and close the returned handle at shutdown. Pass all values of each signature header and the original request stream.

**Spring MVC or Spring Boot** registers the endpoint as a bean. Inside the controller, forward the request to the blocking API:

```kotlin
val result = endpoint.handle(
    request.method,
    request.getHeaders("X-Webhook-Delivery").toList(),
    request.getHeaders("X-Webhook-Signature").toList(),
    request.inputStream
)
return ResponseEntity.status(result.status).apply {
    result.headers.forEach { (name, value) -> header(name, value) }
}.build<Void>()
```

**Ktor 3** mounts `post(endpoint.route)` and uses the suspending API. Import `io.ktor.utils.io.jvm.javaio.toInputStream` to bridge the request channel:

```kotlin
val result = call.receiveChannel().toInputStream().use { body ->
    endpoint.handleAsync(
        "POST",
        call.request.headers.getAll("X-Webhook-Delivery").orEmpty(),
        call.request.headers.getAll("X-Webhook-Signature").orEmpty(),
        body
    )
}
result.headers.forEach { (name, value) -> call.response.headers.append(name, value) }
call.respond(HttpStatusCode.fromValue(result.status))
```

## Verification

- Run `mvn clean verify` with Docker running. Testcontainers starts PostgreSQL, Redis, and MongoDB and the tests set up their own schemas. The Actions workflow supplies PostgreSQL as a service and uses Testcontainers for Redis and MongoDB.
- Run `bash webhook test` for fast core and CLI checks without Docker.
- Run `mvn package` to build artifacts without starting the integration-test containers.
Core contracts live in [library/webhooks](library/webhooks). Storage implementations live in [adapters/archive](adapters/archive) and [adapters/jobs](adapters/jobs). The [CLI and demo handlers](src) are separate from the library.

Licensed under the [MIT License](LICENSE).
