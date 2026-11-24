import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZoneId
import java.util.concurrent.CompletableFuture
import webhooks.*
import webhooks.jobs.JobRecord
import webhooks.jobs.JobRecords
import webhooks.jobs.StoredWebhookJobs

internal fun testPortableQueue() {
    val clock = object : Clock() {
        var now = 1_000L
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = Clock.fixed(instant(), zone)
        override fun instant(): Instant = Instant.ofEpochMilli(now)
    }
    val records = object : JobRecords {
        private val items = java.util.concurrent.ConcurrentHashMap<Pair<String, String>, JobRecord>()
        override fun insert(record: JobRecord) { items.putIfAbsent(record.vendor to record.id, record) }
        override fun get(vendor: String, id: String) = items[vendor to id]
        override fun due(vendor: String, input: JobKind, now: Long) =
            items.values.filter { it.vendor == vendor && it.input == input && (it.due ?: Long.MAX_VALUE) <= now }
        override fun replace(previous: JobRecord, next: JobRecord) =
            items.replace(previous.vendor to previous.id, previous, next)
    }
    val one = StoredWebhookJobs(records, clock)
    val two = StoredWebhookJobs(records, clock)
    val body = byteArrayOf(0, -1, 7)
    one.enqueue(WebhookJob("carrier", "id", JobInput.Projected(body)))
    two.enqueue(WebhookJob("carrier", "id", JobInput.Projected(byteArrayOf(9))))
    check(one.claim("carrier", JobKind.ARCHIVE) == null)
    val first = CompletableFuture.supplyAsync { one.claim("carrier", JobKind.PROJECTED) }
    val second = CompletableFuture.supplyAsync { two.claim("carrier", JobKind.PROJECTED) }
    val claim = listOfNotNull(first.join(), second.join()).single()
    check((claim.job.input as JobInput.Projected).body.contentEquals(body) && claim.attempt == 1)
    clock.now += 300_000
    val recovered = checkNotNull(two.claim("carrier", JobKind.PROJECTED))
    check(recovered.attempt == 2)
    one.complete(claim)
    two.fail(recovered, terminal = false, delaySeconds = 2, failure = "retry")
    check(one.claim("carrier", JobKind.PROJECTED) == null)
    clock.now += 2_000
    val retried = checkNotNull(one.claim("carrier", JobKind.PROJECTED))
    check(retried.attempt == 3)
    one.complete(retried)
    check(one.contains("carrier", "id"))
    check(records.get("carrier", "id")?.body == null)
    check(two.claim("carrier", JobKind.PROJECTED) == null)
    one.enqueue(WebhookJob("carrier", "ref", JobInput.Archived))
    val ref = checkNotNull(two.claim("carrier", JobKind.ARCHIVE))
    check(ref.job.input == JobInput.Archived)
    two.fail(ref, terminal = true, delaySeconds = 0, failure = "permanent")
    check(two.claim("carrier", JobKind.ARCHIVE) == null)
    check(records.get("carrier", "ref")?.failure == "permanent")
    println("PASS portable queue duplicates, exclusive claims, lease fencing, retry timing, and terminal states")
}
