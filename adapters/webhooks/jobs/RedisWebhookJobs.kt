package webhooks.jobs

import java.util.Base64
import redis.clients.jedis.UnifiedJedis
import webhooks.*

/** Durable queue backed by Redis. Use persistent storage and disable eviction of its keys. */
class RedisWebhookJobs(redis: UnifiedJedis, keyPrefix: String = "captain-hooks:queue:") :
    WebhookJobs by StoredWebhookJobs(RedisJobRecords(redis, keyPrefix))

private class RedisJobRecords(private val redis: UnifiedJedis, private val prefix: String) : JobRecords {
    init {
        require('{' !in prefix && '}' !in prefix) { "Redis queue prefix must not contain hash tags." }
    }

    override fun insert(record: JobRecord) {
        redis.eval(
            """
            if redis.call('EXISTS', KEYS[1]) ~= 0 then return 0 end
            redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
            redis.call('SET', KEYS[1], ARGV[1])
            return 1
            """.trimIndent(),
            keys(record), listOf(record.encode(), requireNotNull(record.due).toString(), record.id)
        )
    }

    override fun get(vendor: String, id: String): JobRecord? =
        redis.get(recordKey(vendor, id))?.let { decode(vendor, id, it) }

    override fun due(vendor: String, input: JobInput, now: Long): List<JobRecord> =
        redis.zrangeByScore(indexKey(vendor, input), "-inf", now.toString(), 0, 32)
            .mapNotNull { get(vendor, it) }

    override fun replace(previous: JobRecord, next: JobRecord): Boolean =
        redis.eval(
            """
            local current = redis.call('GET', KEYS[1])
            if not current or current ~= ARGV[1] then return 0 end
            if ARGV[3] == '' then
                redis.call('ZREM', KEYS[2], ARGV[4])
            else
                redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
            end
            redis.call('SET', KEYS[1], ARGV[2])
            return 1
            """.trimIndent(),
            keys(previous), listOf(previous.encode(), next.encode(), next.due?.toString().orEmpty(), next.id)
        ) == 1L

    private fun keys(record: JobRecord) = listOf(recordKey(record.vendor, record.id), indexKey(record.vendor, record.input))
    private fun vendorKey(vendor: String) = "$prefix{${encode(vendor.toByteArray())}}:"
    private fun recordKey(vendor: String, id: String) = vendorKey(vendor) + "job:$id"
    private fun indexKey(vendor: String, input: JobInput) = vendorKey(vendor) + "due:${input.name}"

    private fun JobRecord.encode() = listOf(
        "1", revision, input.name, attempt, due ?: "", if (lease) "1" else "0",
        failure?.let { encode(it.toByteArray()) } ?: "-", body?.let(::encode) ?: "-"
    ).joinToString("|")

    private fun decode(vendor: String, id: String, value: String): JobRecord {
        val parts = value.split('|')
        check(parts.size == 8 && parts[0] == "1")
        return JobRecord(
            vendor, id, JobInput.valueOf(parts[2]), decodeOptional(parts[7]), parts[1],
            parts[3].toInt(), parts[4].toLongOrNull(), parts[5] == "1",
            decodeOptional(parts[6])?.decodeToString()
        )
    }

    private fun encode(value: ByteArray) = Base64.getUrlEncoder().encodeToString(value)
    private fun decodeOptional(value: String) = if (value == "-") null else Base64.getUrlDecoder().decode(value)
}
