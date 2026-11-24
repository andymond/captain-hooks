import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZoneId
import java.util.concurrent.CompletableFuture
import webhooks.*

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
        override fun due(vendor: String, input: JobInput, now: Long) =
            items.values.filter { it.vendor == vendor && it.input == input && (it.due ?: Long.MAX_VALUE) <= now }
        override fun replace(previous: JobRecord, next: JobRecord) =
            items.replace(previous.vendor to previous.id, previous, next)
    }
    val one = StoredWebhookJobs(records, clock)
    val two = StoredWebhookJobs(records, clock)
    val body = byteArrayOf(0, -1, 7)
    one.enqueue("carrier", Delivery("id", byteArrayOf()), body, archive = false)
    two.enqueue("carrier", Delivery("id", byteArrayOf()), byteArrayOf(9), archive = false)
    check(one.claim("carrier", projected = false) == null)
    val first = CompletableFuture.supplyAsync { one.claim("carrier", projected = true) }
    val second = CompletableFuture.supplyAsync { two.claim("carrier", projected = true) }
    val claim = listOfNotNull(first.join(), second.join()).single()
    check(claim.delivery.body.contentEquals(body) && claim.attempt == 1)
    clock.now += 300_000
    val recovered = checkNotNull(two.claim("carrier", projected = true))
    check(recovered.attempt == 2)
    one.complete(claim)
    two.fail(recovered, terminal = false, delaySeconds = 2, failure = "retry")
    check(one.claim("carrier", projected = true) == null)
    clock.now += 2_000
    val retried = checkNotNull(one.claim("carrier", projected = true))
    check(retried.attempt == 3)
    one.complete(retried)
    check(one.contains("carrier", "id"))
    check(records.get("carrier", "id")?.body == null)
    check(two.claim("carrier", projected = true) == null)
    one.enqueueArchiveReference("carrier", "ref")
    val ref = checkNotNull(two.claim("carrier", projected = false))
    check(ref.externalArchive && ref.delivery.body.isEmpty())
    two.fail(ref, terminal = true, delaySeconds = 0, failure = "permanent")
    check(two.claim("carrier", projected = false) == null)
    check(records.get("carrier", "ref")?.failure == "permanent")
    println("PASS portable queue duplicates, exclusive claims, lease fencing, retry timing, and terminal states")
}
