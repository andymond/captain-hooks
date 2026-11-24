# Captain Hooks

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

Async requests return `204` after the job is durably queued. Accepted duplicates also return `204`. Storage failures return `503` so the sender can retry. Workers retry with backoff and retain terminal failures. Use the delivery ID to make external side effects idempotent in case a worker retries after a crash or lease expiry.

## Choose storage

Choose the archive and job queue independently. Applications supply and own their clients.

| Backend | Archive | Job queue | Setup |
| --- | --- | --- | --- |
| JDBC | `JdbcWebhookArchive(dataSource, dialect)` | `JdbcWebhookJobs(dataSource, dialect)` | Supply a JDBC driver and `JdbcDialect` for PostgreSQL, MySQL, MariaDB, H2, or SQLite. Call `createSchema()` or apply equivalent migrations. |
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

Run `mvn install` and depend on `io.github.andymond:captain-hooks:0.1.0-SNAPSHOT` from your local Maven repository. Gradle consumers can add `mavenLocal()`. Add Jedis or the MongoDB Java driver if you use those adapters.

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

- `mvn clean verify` builds the full library. `bash webhook test` runs the project’s core and CLI tests.
- For live JDBC tests, configure the driver and credentials, create the archive schema in a test database, and run `WEBHOOK_TEST_DATABASE_URL="$WEBHOOK_DATABASE_URL" bash webhook test`. Look for the archive and JDBC job storage PASS messages.
- For live Redis or MongoDB checks, wire the adapters into a host and send a duplicate signed request. Expect two `204` responses, one completed job, and one archived body in archive mode.

Core contracts live in [library/webhooks](library/webhooks). Storage implementations live in [adapters/archive](adapters/archive) and [adapters/jobs](adapters/jobs). The [CLI and demo handlers](src) are separate from the library.
