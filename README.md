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
| `ASYNC_ARCHIVE` | The original body loaded from the PostgreSQL archive | `incident-alerts` retains the full alert for attaching it to an incident record. This mode requires `persistWebhooks = true`. |
| `ASYNC_PROJECTED` | The bytes returned by `ProjectedWebhookHandler.project` | `shipment-updates` queues a versioned record with only the order ID, tracking number, and status. The sample leaves archiving off. Enabling it also stores the original body. |

Both async modes verify the signature and commit one job per vendor and delivery ID before returning HTTP `204`. When archiving is enabled, a new job and its archive write commit together. An accepted duplicate returns `204`, malformed projected input returns `400`, and unavailable storage returns `503` with `Retry-After: 1`. Keep projection limited to local parsing and field selection.

Each async server starts one coroutine worker for its vendor. Idle polling suspends, and blocking JDBC and handler calls use the shared I/O dispatcher. Shutdown cancels the worker. A claim interrupted during shutdown can be recovered after its lease expires. Jobs use a five-minute lease and retry handler exceptions with exponential backoff, up to five attempts. A handler can throw `PermanentWebhookFailure` to stop retries immediately. Failed jobs remain available for inspection. When a job completes, its row remains for deduplication and the queue releases its projected payload or archive reference. Keep the archive row while any job still references it. Allow existing jobs to finish before switching a vendor's processing mode, and keep workers compatible with queued projection versions. Switching a vendor from sync to async starts using database deduplication for newly accepted deliveries. Existing filesystem receipts are not imported.

Workers may execute a job again after a crash or lease expiry. Use the delivery ID as the idempotency key for external side effects. Sync mode uses local receipts in `.webhook-data/` or `WEBHOOK_DATA_DIR`. An in-flight sync duplicate returns `503`, and a sync handler failure returns `500` so the sender can retry.

## Run both examples

Start Postgres.app, then create the local database if needed and apply both migrations in order. With a database you already created, start at the first `psql` command.

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
psql -d webhook_dev -c 'SELECT vendor, delivery_id, state, attempts, last_error FROM webhook_jobs;'
psql -d webhook_dev -c 'SELECT vendor, delivery_id, octet_length(body) AS bytes FROM webhook_deliveries;'
```

The generic signing format uses HMAC-SHA256 over the delivery ID, a newline byte, and the raw request body. Send the digest as `sha256=<hex>` in `X-Webhook-Signature`. Use a stable ID of 1–128 ASCII letters, digits, or characters from `._:-` in `X-Webhook-Delivery`.

To also run the PostgreSQL integration tests, point at a test database with both migrations applied. The tests write and clean up their own rows and create a session-local temporary table to exercise rollback.

```sh
WEBHOOK_TEST_DATABASE_URL="$WEBHOOK_DATABASE_URL" ./webhook test
```
