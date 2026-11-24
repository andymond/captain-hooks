import java.sql.Connection
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import webhooks.Delivery
import webhooks.MAX_WEBHOOK_ATTEMPTS
import webhooks.PostgresWebhookJobs
import webhooks.postgresConnectionFactory

internal fun testPostgresJobs() {
    val url = System.getenv("WEBHOOK_TEST_DATABASE_URL") ?: return
    val connect = postgresConnectionFactory(
        url, System.getenv("WEBHOOK_DATABASE_USER"), System.getenv("WEBHOOK_DATABASE_PASSWORD")
    )
    val jobs = PostgresWebhookJobs(connect)
    jobs.verifySchema()
    val vendor = "job-test-${UUID.randomUUID()}"
    val raw = Delivery("alert-1", byteArrayOf(0, -1, 10))
    val shipment = Delivery("shipment-1", "contains-private-fields".toByteArray())
    val projection = "v1\norderId=123".toByteArray()

    fun update(sql: String, id: String) {
        connect().use { connection ->
            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, vendor)
                statement.setString(2, id)
                check(statement.executeUpdate() == 1)
            }
        }
    }

    fun expire(id: String) = update(
        "UPDATE webhook_jobs SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE vendor = ? AND delivery_id = ?",
        id
    )

    fun queryOne(sql: String, id: String): String? = connect().use { connection ->
        connection.prepareStatement(sql).use { statement ->
            statement.setString(1, vendor)
            statement.setString(2, id)
            statement.executeQuery().use { if (it.next()) it.getString(1) else null }
        }
    }

    try {
        // Shadow the archive table on one connection so its insert fails after the job insert.
        val failingArchive = PostgresWebhookJobs {
            connect().also { connection ->
                connection.createStatement().use {
                    it.execute(
                        "CREATE TEMP TABLE webhook_deliveries (" +
                            "vendor varchar(63), delivery_id varchar(128), body bytea CHECK (false), " +
                            "PRIMARY KEY (vendor, delivery_id))"
                    )
                }
            }
        }
        val rejection = runCatching { failingArchive.enqueue(vendor, raw, null, true) }.exceptionOrNull()
        check(rejection is java.sql.SQLException && rejection.sqlState == "23514")
        check(!jobs.contains(vendor, raw.id))

        jobs.enqueue(vendor, raw, null, true)
        jobs.enqueue(vendor, Delivery(raw.id, byteArrayOf(4)), null, true)
        check(jobs.claim(vendor, projected = true) == null)
        val firstClaim = CompletableFuture.supplyAsync { jobs.claim(vendor, projected = false) }
        val secondClaim = CompletableFuture.supplyAsync { jobs.claim(vendor, projected = false) }
        val claimed = listOfNotNull(firstClaim.get(10, TimeUnit.SECONDS), secondClaim.get(10, TimeUnit.SECONDS)).single()
        check(claimed.delivery.body.contentEquals(raw.body))
        check(claimed.attempt == 1)
        expire(raw.id)
        val replacement = checkNotNull(jobs.claim(vendor, projected = false))
        check(replacement.attempt == 2 && replacement.token != claimed.token)
        jobs.complete(claimed)
        val stateSql = "SELECT state FROM webhook_jobs WHERE vendor = ? AND delivery_id = ?"
        check(queryOne(stateSql, raw.id) == "running")
        jobs.complete(replacement)
        check(queryOne(stateSql, raw.id) == "completed")

        jobs.enqueue(vendor, shipment, projection, false)
        val bodySql = "SELECT encode(body, 'hex') FROM webhook_deliveries WHERE vendor = ? AND delivery_id = ?"
        check(queryOne(bodySql, shipment.id) == null)
        val first = checkNotNull(jobs.claim(vendor, projected = true))
        check(first.delivery.body.contentEquals(projection))
        jobs.fail(first, terminal = false, delaySeconds = 3600, failure = "TestFailure")
        check(jobs.claim(vendor, projected = true) == null)
        update(
            "UPDATE webhook_jobs SET available_at = CURRENT_TIMESTAMP WHERE vendor = ? AND delivery_id = ?", shipment.id
        )
        check(checkNotNull(jobs.claim(vendor, projected = true)).attempt == 2)
        update("UPDATE webhook_jobs SET attempts = $MAX_WEBHOOK_ATTEMPTS WHERE vendor = ? AND delivery_id = ?", shipment.id)
        expire(shipment.id)
        check(jobs.claim(vendor, projected = true) == null)
        check(queryOne(stateSql, shipment.id) == "failed")
    } finally {
        connect().use { connection ->
            deleteVendor(connection, "webhook_jobs", vendor)
            deleteVendor(connection, "webhook_deliveries", vendor)
        }
    }
    println("PASS PostgreSQL jobs: archive and projection, exclusive claims, lease recovery, and retry state")
}

private fun deleteVendor(connection: Connection, table: String, vendor: String) {
    connection.prepareStatement("DELETE FROM $table WHERE vendor = ?").use { statement ->
        statement.setString(1, vendor)
        statement.executeUpdate()
    }
}
