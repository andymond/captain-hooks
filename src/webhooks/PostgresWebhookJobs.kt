package webhooks

import java.sql.Connection
import java.sql.PreparedStatement
import java.util.UUID

internal class PostgresWebhookJobs(private val connect: () -> Connection) : WebhookJobs {
    fun verifySchema() {
        connect().use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(
                    """
                    SELECT j.state, j.attempts, j.available_at, j.lease_until, j.lease_token,
                           j.last_error, j.projected_body, a.body
                    FROM webhook_jobs j
                    LEFT JOIN webhook_deliveries a ON (a.vendor, a.delivery_id) = (j.vendor, j.archive_delivery_id)
                    WHERE FALSE
                    """.trimIndent()
                ).close()
            }
        }
    }

    override fun enqueue(vendor: String, delivery: Delivery, projected: ByteArray?, archive: Boolean) {
        require(projected != null || archive)
        connect().use { connection ->
            connection.autoCommit = false
            try {
                val inserted = connection.prepareStatement(
                    """
                    INSERT INTO webhook_jobs (vendor, delivery_id, projected_body, archive_delivery_id)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (vendor, delivery_id) DO NOTHING
                    """.trimIndent()
                ).use { statement ->
                    statement.setString(1, vendor)
                    statement.setString(2, delivery.id)
                    statement.setBytes(3, projected)
                    statement.setString(4, delivery.id.takeIf { projected == null })
                    statement.executeUpdate() == 1
                }
                if (inserted && archive) insertArchive(connection, vendor, delivery)
                connection.commit()
            } catch (failure: Exception) {
                try {
                    connection.rollback()
                } catch (rollbackFailure: Exception) {
                    failure.addSuppressed(rollbackFailure)
                }
                throw failure
            }
        }
    }

    override fun contains(vendor: String, id: String): Boolean = connect().use { connection ->
        connection.prepareStatement(
            "SELECT 1 FROM webhook_jobs WHERE vendor = ? AND delivery_id = ?"
        ).use { statement ->
            statement.setString(1, vendor)
            statement.setString(2, id)
            statement.executeQuery().use { it.next() }
        }
    }

    override fun claim(vendor: String, projected: Boolean): ClaimedWebhook? = connect().use { connection ->
        val token = UUID.randomUUID()
        connection.prepareStatement(
            """
            WITH expired AS (
                SELECT vendor, delivery_id FROM webhook_jobs
                WHERE vendor = ? AND state = 'running' AND lease_until <= CURRENT_TIMESTAMP AND attempts >= ?
                FOR UPDATE SKIP LOCKED
            ), exhausted AS (
                UPDATE webhook_jobs j
                SET state = 'failed', lease_token = NULL, lease_until = NULL, last_error = 'LeaseExpired'
                FROM expired e WHERE (j.vendor, j.delivery_id) = (e.vendor, e.delivery_id)
            ), next_job AS (
                SELECT vendor, delivery_id FROM webhook_jobs
                WHERE vendor = ? AND attempts < ? AND (projected_body IS NOT NULL) = ?
                  AND ((state = 'pending' AND available_at <= CURRENT_TIMESTAMP)
                    OR (state = 'running' AND lease_until <= CURRENT_TIMESTAMP))
                ORDER BY available_at, delivery_id
                LIMIT 1 FOR UPDATE SKIP LOCKED
            ), claimed AS (
                UPDATE webhook_jobs j
                SET state = 'running', attempts = attempts + 1, lease_token = ?,
                    lease_until = CURRENT_TIMESTAMP + INTERVAL '5 minutes'
                FROM next_job n
                WHERE (j.vendor, j.delivery_id) = (n.vendor, n.delivery_id)
                RETURNING j.*
            )
            SELECT c.delivery_id, c.attempts, COALESCE(c.projected_body, a.body)
            FROM claimed c LEFT JOIN webhook_deliveries a
                ON (a.vendor, a.delivery_id) = (c.vendor, c.archive_delivery_id)
            """.trimIndent()
        ).use { statement ->
            statement.setString(1, vendor)
            statement.setInt(2, MAX_WEBHOOK_ATTEMPTS)
            statement.setString(3, vendor)
            statement.setInt(4, MAX_WEBHOOK_ATTEMPTS)
            statement.setBoolean(5, projected)
            statement.setObject(6, token)
            statement.executeQuery().use { rows ->
                if (!rows.next()) null
                else ClaimedWebhook(vendor, Delivery(rows.getString(1), rows.getBytes(3)), rows.getInt(2), token)
            }
        }
    }

    override fun complete(job: ClaimedWebhook) {
        updateClaim(
            job, """
            state = 'completed', projected_body = NULL, archive_delivery_id = NULL, last_error = NULL
            """.trimIndent()
        ) { }
    }

    override fun fail(job: ClaimedWebhook, terminal: Boolean, delaySeconds: Int, failure: String) {
        updateClaim(
            job, """
            state = ?, available_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'), last_error = ?
            """.trimIndent(), parameters = 3
        ) { statement ->
            statement.setString(1, if (terminal) "failed" else "pending")
            statement.setInt(2, delaySeconds)
            statement.setString(3, failure)
        }
    }

    private fun updateClaim(
        job: ClaimedWebhook,
        assignments: String,
        parameters: Int = 0,
        bind: (PreparedStatement) -> Unit
    ) {
        connect().use { connection ->
            connection.prepareStatement(
                """
                UPDATE webhook_jobs SET $assignments, lease_token = NULL, lease_until = NULL
                WHERE vendor = ? AND delivery_id = ? AND lease_token = ? AND state = 'running'
                  AND lease_until > CURRENT_TIMESTAMP
                """.trimIndent()
            ).use { statement ->
                bind(statement)
                statement.setString(parameters + 1, job.vendor)
                statement.setString(parameters + 2, job.delivery.id)
                statement.setObject(parameters + 3, job.token)
                statement.executeUpdate()
            }
        }
    }
}
