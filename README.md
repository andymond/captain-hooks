# Captain Hooks

Use JDK 17 or newer and a Kotlin compiler that supports JVM target 17. The launcher uses `kotlinc` on `PATH`, an explicit `KOTLINC` executable, or the compiler bundled with IntelliJ IDEA at its default macOS location. It loads `kotlinx-coroutines-core-jvm` from the compiler distribution. If that distribution omits the JAR, set `WEBHOOK_COROUTINES_JAR` to its absolute path. The IntelliJ module declares version 1.8.0, which matches the bundled JAR used to test the launcher.

On a fresh clone, run `chmod +x webhook` if the launcher needs executable permission.

```sh
./webhook test
./webhook generate my-vendor
```

The generator creates `src/webhooks/<namespace>/Handler.kt` and protects existing handlers from overwrites. Vendor names are normalized to lowercase, with hyphens mapped to underscores in Kotlin namespaces. The server binds to loopback and runs one vendor per process. The default port is `8080`.

## Processing modes

| Mode | What the handler receives | Example |
| --- | --- | --- |
| `SYNC` | The original body during the request | Default for generated handlers. Enable `persistWebhooks` to archive verified deliveries before handling. |
| `ASYNC_ARCHIVE` | The original body loaded from the configured archive | `incident-alerts` retains the full alert for attaching it to an incident record. This mode requires `persistWebhooks = true`. |
| `ASYNC_PROJECTED` | The bytes returned by `ProjectedWebhookHandler.project` | `shipment-updates` queues a versioned record with only the order ID, tracking number, and status. The sample leaves archiving off. Enabling it also stores the original body. |

Both async modes verify the signature and durably enqueue one job per vendor and delivery ID before returning HTTP `204`. Archive mode saves the original body before enqueueing its reference. An accepted duplicate returns `204`, malformed projected input returns `400`, and unavailable storage returns `503` with `Retry-After: 1`. Keep projection limited to local parsing and field selection.

Each async server starts one coroutine worker for its vendor. Idle polling suspends, and blocking JDBC and handler calls use the shared I/O dispatcher. Shutdown cancels the worker. A claim interrupted during shutdown can be recovered after its lease expires. Jobs use a five-minute lease and retry handler exceptions with exponential backoff, up to five attempts. A handler can throw `PermanentWebhookFailure` to stop retries immediately. Failed jobs remain available for inspection. When a job completes, its row remains for deduplication and the queue releases its projected payload or archive reference. Keep the archive row while any job still references it. Allow existing jobs to finish before switching a vendor's processing mode, and keep workers compatible with queued projection versions. Switching a vendor from sync to async starts using database deduplication for newly accepted deliveries. Existing filesystem receipts are not imported.

Workers may execute a job again after a crash or lease expiry. Use the delivery ID as the idempotency key for external side effects. Sync mode uses local receipts in `.webhook-data/` or `WEBHOOK_DATA_DIR`. An in-flight sync duplicate returns `503`, and a sync handler failure returns `500` so the sender can retry.

## Run both examples

Start Postgres.app, then create the local database if needed and apply the migrations in order. With a database you already created, start at the first `psql` command.

```sh
createdb webhook_dev
psql -d webhook_dev -v ON_ERROR_STOP=1 -f db/001_webhook_deliveries.sql
psql -d webhook_dev -v ON_ERROR_STOP=1 -f db/002_webhook_jobs.sql
```

Download the [PostgreSQL JDBC driver](https://jdbc.postgresql.org/download/) and point the launcher at its JAR. Set `WEBHOOK_DATABASE_PASSWORD` if your PostgreSQL role requires a password.

```sh
export WEBHOOK_JDBC_JAR=/absolute/path/to/postgresql.jar
export WEBHOOK_DATABASE_URL=jdbc:postgresql://127.0.0.1:5432/webhook_dev
export WEBHOOK_DATABASE_USER="$(id -un)"
```

Run each server in a terminal with those variables set.

```sh
WEBHOOK_SECRET=demo-secret ./webhook serve incident-alerts 8080
WEBHOOK_SECRET=demo-secret ./webhook serve shipment-updates 8081
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

Both endpoints return `204`. The incident worker reports the size of the original alert. The shipment worker reports the order, tracking number, and status. Only the incident has a row in the raw archive.

```sh
psql -d webhook_dev -c 'SELECT vendor, delivery_id, attempts, due_at, leased, last_error FROM captain_webhook_jobs;'
psql -d webhook_dev -c 'SELECT vendor, delivery_id, octet_length(body) AS bytes FROM webhook_deliveries;'
```

The generic signing format uses HMAC-SHA256 over the delivery ID, a newline byte, and the raw request body. Send the digest as `sha256=<hex>` in `X-Webhook-Signature`. Use a stable ID of 1–128 ASCII letters, digits, or characters from `._:-` in `X-Webhook-Delivery`.

To also run the PostgreSQL integration tests, point at a test database with the migrations applied. The tests write and clean up their own rows and exercise JDBC archive and queue behavior.

```sh
WEBHOOK_TEST_DATABASE_URL="$WEBHOOK_DATABASE_URL" ./webhook test
```

## Embed Captain Hooks in Spring or Ktor

Run `mvn install` to build and install `io.github.andymond:captain-hooks:0.1.0-SNAPSHOT` in your Maven local repository. In Gradle, add `mavenLocal()` to repositories and use `implementation("io.github.andymond:captain-hooks:0.1.0-SNAPSHOT")`. The artifact contains the core, archive and queue adapters, and SQL migrations. The CLI and sample handlers are separate. For an offline core and JDBC JAR, run `./webhook build-core`.

Construct a `webhooks.WebhookEndpoint` with your handler, signature secret, and a directory for synchronous delivery receipts. Pass an archive when enabling `persistWebhooks`. Hosts running an async handler also pass a `WebhookJobs` implementation and close the handle returned by `endpoint.startWorker()` on shutdown.

### Choose an archive

| Archive | Construction | Setup |
| --- | --- | --- |
| SQL | `JdbcWebhookArchive(dataSource, JdbcDialect.POSTGRESQL)` | Supply the application's pooled JDBC `DataSource` and driver. Other dialects are `MYSQL`, `MARIADB`, `H2`, and `SQLITE`. Run `createSchema()` during setup or apply an equivalent migration. |
| Redis | `RedisWebhookArchive(redis)` | Add `redis.clients:jedis:5.2.0` and pass an application-managed `UnifiedJedis` client. Use persistent Redis with an eviction policy that preserves archive keys. |
| MongoDB | `MongoWebhookArchive(database.getCollection("webhook_deliveries"))` | Add `org.mongodb:mongodb-driver-sync:5.5.0` and pass a `MongoCollection<Document>` with acknowledged writes. |

`JdbcWebhookArchive`, `RedisWebhookArchive`, and `MongoWebhookArchive` save the first payload for each vendor and delivery ID. All archive adapters live in `webhooks.archive`. The shared SQL dialect enum is `webhooks.jdbc.JdbcDialect`. The host owns each database client and its credentials, pooling, durability, and shutdown configuration.

### Choose a job queue independently

| Queue | Construction | Setup |
| --- | --- | --- |
| SQL | `JdbcWebhookJobs(dataSource, JdbcDialect.H2)` | Use any of the archive SQL dialects. Call `createSchema()` during setup. For high-volume polling, index `captain_webhook_jobs(vendor, input_kind, due_at)` in your migrations. |
| Redis | `RedisWebhookJobs(redis)` | Supply an application-managed Jedis client. Use persistent, non-evicting Redis. |
| MongoDB | `MongoWebhookJobs(database.getCollection("webhook_jobs"))` | Supply an acknowledged collection and call `createIndexes()` during setup. |

All queue adapters live in `webhooks.jobs`. Configure a `WebhookArchive` independently when an async handler has `persistWebhooks = true`. You can use a JDBC queue with a MongoDB archive, a Redis queue with a JDBC archive, or another combination. Projected handlers with archiving disabled need only a queue.

With an independent archive, saving the payload and enqueueing are two writes. If the save succeeds and enqueueing fails, the archive retains the original and the endpoint returns `503`. A sender retry safely reuses that record. Keep archived bodies until all jobs that reference them can finish retrying. Queue workers use the host clocks for lease times, so synchronize clocks across worker instances.

The CLI demos use `JdbcWebhookJobs` and `JdbcWebhookArchive` with the PostgreSQL dialect.

### Code layout

- `library/webhooks` defines the endpoint, handler, worker, and storage interfaces. `WebhookJob` carries either `JobInput.Projected` bytes or an `Archived` reference identified by its vendor and delivery ID.
- `adapters/archive` contains the JDBC, Redis, and MongoDB implementations of first-write-wins payload storage.
- `adapters/jobs` contains the JDBC, Redis, and MongoDB queues. Its shared code handles attempts, retry scheduling, and lease expiry. Each backend implements atomic record insertion and compare-and-set updates.
- `adapters/jdbc` contains the SQL types and error handling shared by the JDBC adapters.
- `src` contains the CLI, JDK HTTP bridge, and demo handlers.

The JDBC queue polls due rows in `captain_webhook_jobs` and claims one by updating it only if its revision still matches. The resulting revision also fences completion and retry updates. SQL dialect selection controls binary column types, key collation, and duplicate-key detection. The claim query and update are shared across dialects.

Custom queues implement `WebhookJobs`. The endpoint saves archived deliveries and the worker reads them through `ReadableWebhookArchive`. Queue implementations store the job input and have no archive dependency.


### Spring MVC or Spring Boot

Register the endpoint as a Spring bean and mount its route. Pass the original request stream and all header values. For async mode, register the worker as a bean with `destroyMethod = "close"` so Spring owns its lifecycle.

For example, this Spring Boot configuration uses the application's JDBC `DataSource` and a synchronous handler with `persistWebhooks = true`. Apply the archive schema before accepting traffic. Replace the archive bean with one of the constructors above to change storage.

```kotlin
import java.nio.file.Path
import javax.sql.DataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import webhooks.jdbc.JdbcDialect
import webhooks.archive.JdbcWebhookArchive
import webhooks.WebhookArchive
import webhooks.WebhookEndpoint
import webhooks.WebhookHandler

@Configuration(proxyBeanMethods = false)
class WebhookConfiguration {
    @Bean
    fun archive(dataSource: DataSource): WebhookArchive =
        JdbcWebhookArchive(dataSource, JdbcDialect.POSTGRESQL)

    @Bean
    fun endpoint(
        handler: WebhookHandler,
        archive: WebhookArchive,
        @Value("\${webhook.secret}") secret: String
    ) = WebhookEndpoint(handler, secret.toByteArray(), Path.of(".webhook-data"), archive)
}
```

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
