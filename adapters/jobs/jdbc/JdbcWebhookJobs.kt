package webhooks.jobs

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import javax.sql.DataSource
import webhooks.JobKind
import webhooks.WebhookJobs
import webhooks.jdbc.JdbcDialect
import webhooks.jdbc.isDuplicate
import webhooks.jdbc.keyCollation

/** JDBC queue with database-independent claim and lease semantics. */
class JdbcWebhookJobs private constructor(private val records: JdbcJobRecords) :
    WebhookJobs by StoredWebhookJobs(records) {
    constructor(dataSource: DataSource, dialect: JdbcDialect) : this(dialect, { dataSource.connection })
    constructor(dialect: JdbcDialect, connect: () -> Connection) : this(JdbcJobRecords(dialect, connect))

    fun createSchema() = records.createSchema()
}

private class JdbcJobRecords(
    private val dialect: JdbcDialect,
    private val connect: () -> Connection
) : JobRecords {
    fun createSchema() = write { connection ->
        connection.createStatement().use {
            it.executeUpdate(
                """
                CREATE TABLE IF NOT EXISTS captain_webhook_jobs (
                    vendor varchar(63)${dialect.keyCollation} NOT NULL, delivery_id varchar(128)${dialect.keyCollation} NOT NULL,
                    input_kind varchar(16) NOT NULL, body ${dialect.bodyType}, revision varchar(36) NOT NULL,
                    attempts integer NOT NULL, due_at bigint, leased boolean NOT NULL, last_error varchar(255),
                    PRIMARY KEY (vendor, delivery_id)
                )
                """.trimIndent()
            )
        }
        Unit
    }

    override fun insert(record: JobRecord) {
        try {
            write { connection ->
                connection.prepareStatement(
                    """
                    INSERT INTO captain_webhook_jobs
                        (input_kind, body, revision, attempts, due_at, leased, last_error, vendor, delivery_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """.trimIndent()
                ).use { it.bind(record); it.executeUpdate() }
            }
        } catch (failure: SQLException) {
            if (!dialect.isDuplicate(failure) || get(record.vendor, record.id) == null) throw failure
        }
    }

    override fun get(vendor: String, id: String): JobRecord? = connect().use { connection ->
        connection.prepareStatement("SELECT * FROM captain_webhook_jobs WHERE vendor = ? AND delivery_id = ?").use {
            it.setString(1, vendor)
            it.setString(2, id)
            it.executeQuery().use { rows -> if (rows.next()) rows.job() else null }
        }
    }

    override fun due(vendor: String, input: JobKind, now: Long): List<JobRecord> = connect().use { connection ->
        connection.prepareStatement(
            """
            SELECT * FROM captain_webhook_jobs WHERE vendor = ? AND input_kind = ? AND due_at <= ?
            ORDER BY due_at, delivery_id
            """.trimIndent()
        ).use {
            it.maxRows = 32
            it.setString(1, vendor)
            it.setString(2, input.name)
            it.setLong(3, now)
            it.executeQuery().use { rows -> buildList { while (rows.next()) add(rows.job()) } }
        }
    }

    override fun replace(previous: JobRecord, next: JobRecord): Boolean = write { connection ->
        connection.prepareStatement(
            """
            UPDATE captain_webhook_jobs SET input_kind = ?, body = ?, revision = ?, attempts = ?,
                due_at = ?, leased = ?, last_error = ?
            WHERE vendor = ? AND delivery_id = ? AND revision = ?
            """.trimIndent()
        ).use {
            it.bind(next)
            it.setString(10, previous.revision)
            it.executeUpdate() == 1
        }
    }

    private fun <T> write(action: (Connection) -> T): T = connect().use { connection ->
        val result = action(connection)
        if (!connection.autoCommit) connection.commit()
        result
    }

    private fun PreparedStatement.bind(record: JobRecord) {
        setString(1, record.input.name)
        setBytes(2, record.body)
        setString(3, record.revision)
        setInt(4, record.attempt)
        val due = record.due
        if (due == null) setNull(5, Types.BIGINT) else setLong(5, due)
        setBoolean(6, record.lease)
        setString(7, record.failure)
        setString(8, record.vendor)
        setString(9, record.id)
    }

    private fun ResultSet.job(): JobRecord {
        val due = getLong("due_at").let { if (wasNull()) null else it }
        return JobRecord(
            getString("vendor"), getString("delivery_id"), JobKind.valueOf(getString("input_kind")),
            getBytes("body"), getString("revision"), getInt("attempts"), due,
            getBoolean("leased"), getString("last_error")
        )
    }
}
