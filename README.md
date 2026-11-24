# Captain Hooks

A JVM library and CLI for receiving signed webhooks, deduplicating deliveries, and running handlers in the request or in the background. Applications choose the archive and job queue independently.

## Quick start without a database

Use JDK 17+ and a Kotlin compiler that supports JVM target 17. The launcher finds `kotlinc` on `PATH` or uses the compiler bundled with IntelliJ IDEA in its default macOS location. Set `KOTLINC` to select another compiler. Set `WEBHOOK_COROUTINES_JAR` if your compiler distribution omits `kotlinx-coroutines-core-jvm`.

```sh
bash webhook test
bash webhook generate my-vendor
WEBHOOK_SECRET=demo-secret bash webhook serve my-vendor
```

The generator creates `src/webhooks/<namespace>/Handler.kt` and protects existing handlers from overwrites. Generated handlers start in synchronous mode with archiving disabled and use local files for deduplication. The CLI serves one vendor per process on loopback, with port `8080` by default.

## Processing modes

| Mode | What the handler receives | Example |
| --- | --- | --- |
| `SYNC` | The original body during the request | Default for generated handlers. Enable `persistWebhooks` to archive verified deliveries before handling. |
| `ASYNC_ARCHIVE` | The original body loaded from the configured archive | `incident-alerts` retains the full alert for attaching it to an incident record. This mode requires `persistWebhooks = true`. |
| `ASYNC_PROJECTED` | The bytes returned by `ProjectedWebhookHandler.project` | `shipment-updates` queues a versioned record with only the order ID, tracking number, and status. The sample leaves archiving off. Enabling it also stores the original body. |

Both async modes verify the signature and durably enqueue one job per vendor and delivery ID before returning HTTP `204`. Archive mode saves the original body before enqueueing its reference. An accepted duplicate returns `204`, malformed projected input returns `400`, and unavailable storage returns `503` with `Retry-After: 1`. Keep projection limited to local parsing and field selection.

Each async server starts one coroutine worker for its vendor. Idle polling suspends, and blocking storage and handler calls use the shared I/O dispatcher. Shutdown cancels the worker. A claim interrupted during shutdown can be recovered after its lease expires. Jobs use a five-minute lease and retry handler exceptions with exponential backoff, up to five attempts. A handler can throw `PermanentWebhookFailure` to stop retries immediately. Failed jobs remain available for inspection. When a job completes, its row remains for deduplication and the queue releases its projected payload or archive reference. Keep the archive row while any job still references it. Allow existing jobs to finish before switching a vendor's processing mode, and keep workers compatible with queued projection versions. Switching a vendor from sync to async starts using database deduplication for newly accepted deliveries. Existing filesystem receipts are not imported.

Workers may execute a job again after a crash or lease expiry. Use the delivery ID as the idempotency key for external side effects. Sync mode uses local receipts in `.webhook-data/` or `WEBHOOK_DATA_DIR`. An in-flight sync duplicate returns `503`, and a sync handler failure returns `500` so the sender can retry.

## Choose storage

| Archive | Construction | Setup |
| --- | --- | --- |
| SQL | `JdbcWebhookArchive(dataSource, dialect)` | Supply the application's pooled JDBC `DataSource` and driver. Choose `POSTGRESQL`, `MYSQL`, `MARIADB`, `H2`, or `SQLITE`. Run `createSchema()` during setup or apply an equivalent migration. |
| Redis | `RedisWebhookArchive(redis)` | Add `redis.clients:jedis:5.2.0` and pass an application-managed `UnifiedJedis` client. Use persistent Redis with an eviction policy that preserves archive keys. |
| MongoDB | `MongoWebhookArchive(database.getCollection("webhook_deliveries"))` | Add `org.mongodb:mongodb-driver-sync:5.5.0` and pass a `MongoCollection<Document>` with acknowledged writes. |

`JdbcWebhookArchive`, `RedisWebhookArchive`, and `MongoWebhookArchive` save the first payload for each vendor and delivery ID. All archive adapters live in `webhooks.archive`. The shared SQL dialect enum is `webhooks.jdbc.JdbcDialect`. The host owns each database client and its credentials, pooling, durability, and shutdown configuration.

### Select a job queue independently

| Queue | Construction | Setup |
| --- | --- | --- |
| SQL | `JdbcWebhookJobs(dataSource, dialect)` | Use any of the archive SQL dialects. Call `createSchema()` during setup. For high-volume polling, index `captain_webhook_jobs(vendor, input_kind, due_at)` in your migrations. |
| Redis | `RedisWebhookJobs(redis)` | Supply an application-managed Jedis client. Use persistent, non-evicting Redis. |
| MongoDB | `MongoWebhookJobs(database.getCollection("webhook_jobs"))` | Supply an acknowledged collection and call `createIndexes()` during setup. |

All queue adapters live in `webhooks.jobs`. Configure a `WebhookArchive` independently when an async handler has `persistWebhooks = true`. You can use a JDBC queue with a MongoDB archive, a Redis queue with a JDBC archive, or another combination. Projected handlers with archiving disabled need only a queue.

With an independent archive, saving the payload and enqueueing are two writes. If the save succeeds and enqueueing fails, the archive retains the original and the endpoint returns `503`. A sender retry safely reuses that record. Keep archived bodies until all jobs that reference them can finish retrying. Queue workers use the host clocks for lease times, so synchronize clocks across worker instances.

## Configure storage for the CLI demos

The CLI uses the JDBC adapters for async processing and archiving. Set these values in each server terminal:

| Setting | Value |
| --- | --- |
| `WEBHOOK_DATABASE_URL` | A JDBC URL for a database using one of the supported dialects |
| `WEBHOOK_JDBC_JAR` | Absolute path to the matching JDBC driver JAR |
| `WEBHOOK_DATABASE_USER` | Database username, if required |
| `WEBHOOK_DATABASE_PASSWORD` | Database password, if required |

Create the database if your engine needs one. The CLI selects the dialect from the URL and creates the required tables if they are missing. The CLI connection must be allowed to run the schema initializer. Embedded applications can apply equivalent migrations separately using their own setup role. The scripts in `db/` are the PostgreSQL schema example.

For Redis, MongoDB, or different queue and archive backends, configure the adapters in your host application using the storage table above.

### Send signed demo deliveries

After configuring JDBC, run each server in a separate terminal with the same storage settings.

```sh
WEBHOOK_SECRET=demo-secret bash webhook serve incident-alerts 8080
WEBHOOK_SECRET=demo-secret bash webhook serve shipment-updates 8081
```

In a third terminal, sign and send each example. Run this block again with the same IDs to see the duplicate acknowledgments.

```sh
send_hook() {
  id=$1
  body=$2
  url=$3
  signature=$(printf '%s\n%s' "$id" "$body" |
    openssl dgst -sha256 -hmac demo-secret | awk '{print $NF}')
  curl -sS -o /dev/null -w '%{http_code}\n' \
    -H "X-Webhook-Delivery: $id" \
    -H "X-Webhook-Signature: sha256=$signature" \
    --data-binary "$body" "$url"
}

send_hook alert-1 'Checkout is down' http://127.0.0.1:8080/webhooks/incident-alerts
send_hook shipment-1 'orderId=123&trackingNumber=ZX9&status=shipped&address=42+Private+Lane' \
  http://127.0.0.1:8081/webhooks/shipment-updates
```

Both endpoints return `204`. The incident worker reports the size of the original alert. The shipment worker reports the order, tracking number, and status. The original incident body is the only payload written to the archive.

The generic signing format uses HMAC-SHA256 over the delivery ID, a newline byte, and the raw request body. Send the digest as `sha256=<hex>` in `X-Webhook-Signature`. Use a stable ID of 1–128 ASCII letters, digits, or characters from `._:-` in `X-Webhook-Delivery`.

## Embed Captain Hooks in Spring or Ktor

Run `mvn install` to build and install `io.github.andymond:captain-hooks:0.1.0-SNAPSHOT` in your Maven local repository. In Gradle, add `mavenLocal()` to repositories and use `implementation("io.github.andymond:captain-hooks:0.1.0-SNAPSHOT")`. The artifact includes the core, archive and queue adapters, and SQL scripts. Use `bash webhook build-core` for an offline core and JDBC JAR.

Construct a `WebhookEndpoint` with your handler, signature secret, and a directory for synchronous receipts. Supply an archive bean or object if your handler enables `persistWebhooks`. Archive mode requires a `ReadableWebhookArchive`. Supply a queue for async processing, start the worker with `endpoint.startWorker()`, and close the returned handle on shutdown.

### Spring MVC or Spring Boot

Register the endpoint and whichever storage adapters the handler needs as Spring beans. This endpoint configuration accepts the storage beans selected by your application.

```kotlin
import java.nio.file.Path
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import webhooks.WebhookArchive
import webhooks.WebhookEndpoint
import webhooks.WebhookHandler
import webhooks.WebhookJobs

@Configuration(proxyBeanMethods = false)
class WebhookConfiguration {
    @Bean
    fun endpoint(
        handler: WebhookHandler,
        archive: ObjectProvider<WebhookArchive>,
        jobs: ObjectProvider<WebhookJobs>,
        @Value("\${webhook.secret}") secret: String,
        @Value("\${webhook.receipts:.webhook-data}") receipts: String
    ) = WebhookEndpoint(
        handler, secret.toByteArray(), Path.of(receipts),
        archive.getIfAvailable(), jobs.getIfAvailable()
    )
}
```

For an async Spring application, register `endpoint.startWorker()` as a bean with `destroyMethod = "close"`. Pass the original request stream and every value of the delivery and signature headers to the endpoint.

Supply a handler bean with vendor `shipment-updates` for this controller.

```kotlin
import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import webhooks.WebhookEndpoint

@RestController
class ShipmentWebhookController(private val endpoint: WebhookEndpoint) {
    @PostMapping("/webhooks/shipment-updates")
    fun receive(request: HttpServletRequest): ResponseEntity<Void> {
        val result = endpoint.handle(
            request.method,
            request.getHeaders("X-Webhook-Delivery").toList(),
            request.getHeaders("X-Webhook-Signature").toList(),
            request.inputStream
        )
        return ResponseEntity.status(result.status).apply {
            result.headers.forEach { (name, value) -> header(name, value) }
        }.build()
    }
}
```

### Ktor 3

Use the suspend entry point so body reads and blocking archive or queue calls run on the I/O dispatcher.

```kotlin
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.request.receiveChannel
import io.ktor.server.response.respond
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.utils.io.jvm.javaio.toInputStream
import webhooks.WebhookEndpoint

fun Application.installWebhook(endpoint: WebhookEndpoint) {
    val worker = endpoint.startWorker()
    monitor.subscribe(ApplicationStopped) { worker?.close() }
    routing {
        post(endpoint.route) {
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
        }
    }
}
```

## Verify the build and storage

- Run `mvn clean verify` to compile and package all adapters.
- Run `bash webhook test` for the core and CLI test suite. To enable the live JDBC checks, configure the driver and credentials above, provision the archive table in a dedicated test database, and set `WEBHOOK_TEST_DATABASE_URL` to its JDBC URL before running the suite. Look for the archive and JDBC job storage PASS messages.
- For a live Redis or MongoDB smoke test, mount a handler with that backend in Spring or Ktor. Send the same signed delivery twice and check for two `204` responses, one completed job, and the original archived bytes. In projected mode with archiving disabled, check that only the projected fields reach the worker.

## Code layout

- `library/webhooks` defines the endpoint, handler, worker, and storage interfaces. `WebhookJob` carries either `JobInput.Projected` bytes or a `JobInput.Archived` reference identified by its vendor and delivery ID.
- `adapters/archive` contains the JDBC, Redis, and MongoDB implementations of first-write-wins payload storage.
- `adapters/jobs` contains the JDBC, Redis, and MongoDB queues. Its shared code handles attempts, retry scheduling, and lease expiry. Each backend implements atomic record insertion and compare-and-set updates.
- `adapters/jdbc` contains the SQL types and error handling shared by the JDBC adapters.
- `src` contains the CLI, JDK HTTP bridge, and demo handlers.

The JDBC queue polls due rows in `captain_webhook_jobs` and claims one by updating it only if its revision still matches. The resulting revision also fences completion and retry updates. SQL dialect selection controls binary column types, key collation, and duplicate-key detection. The claim query and update are shared across dialects.

Custom queues implement `WebhookJobs`. The endpoint saves archived deliveries and the worker reads them through `ReadableWebhookArchive`. Queue implementations store the job input and have no archive dependency.
