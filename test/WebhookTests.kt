import java.util.concurrent.CompletableFuture
import java.util.HexFormat
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import webhooks.WebhookArchive
import webhooks.Delivery
import webhooks.WebhookHandler
import webhooks.WebhookEndpoint
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively

@OptIn(ExperimentalPathApi::class)
internal fun withTempDirectory(test: (Path) -> Unit) {
    val directory = Files.createTempDirectory("webhook-test")
    try {
        test(directory)
    } finally {
        directory.deleteRecursively()
    }
}

fun main() {
    withTempDirectory { directory ->
        val output = mutableListOf<String>()
        fun run(vararg args: String) = runCli(arrayOf(*args), directory, output::add)
        check(run("generate", "Example-Co") == 0)
        val generated = directory.resolve("src/webhooks/example_co/Handler.kt")
        val source = Files.readString(generated)
        check(source.startsWith("package webhooks.example_co\n"))
        check(source.contains("override val vendor = \"example-co\""))
        check(source.contains("override val persistWebhooks = false"))
        check(run("generate", "example-co") != 0)
        check(Files.readString(generated) == source)
        check(run("generate", "../outside") != 0)
        check(run("generate") != 0)
    }
    println("PASS CLI generation, validation, and overwrite protection")
    testEndpoint()
    testAsyncWebhooks()
    testLibrary()
    testPortableQueue()
    testWebhookPersistence()
    testPostgresArchive()
    testJdbcConnectionSelection()
    testJdbcJobs()
}

private fun testEndpoint() = withTempDirectory { directory ->
    val secret = "test-secret".toByteArray()
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val calls = AtomicInteger()
    val retries = AtomicInteger()
    val handler = object : WebhookHandler {
        override val vendor = "example"

        override fun handle(delivery: Delivery) {
            check(delivery.body.contentEquals(byteArrayOf(0, -1, 10)))
            calls.incrementAndGet()
            if (delivery.id == "slow") {
                entered.countDown()
                check(release.await(10, TimeUnit.SECONDS))
            }
            if (delivery.id == "retry" && retries.incrementAndGet() == 1) error("test failure")
        }
    }
    fun endpoint() = WebhookEndpoint(handler, secret, directory)
    val first = endpoint()
    val body = byteArrayOf(0, -1, 10)
    fun status(
        id: String, signedId: String = id, payload: ByteArray = body,
        path: String = "/webhooks/example", method: String = "POST", signed: Boolean = true,
        receiver: WebhookEndpoint = first
    ) = receiver.receive(
        method, path, id, if (signed) sign(secret, signedId, body) else null, payload.inputStream()
    )

    check(status("first", signed = false) == 401)
    check(status("first", signedId = "other") == 401)
    check(status("first", payload = byteArrayOf(1)) == 401)
    check(status("first", path = "/webhooks/example/extra") == 404)
    check(status("first", method = "GET") == 405)
    check(status("first", payload = ByteArray(1024 * 1024 + 1)) == 413)
    check(calls.get() == 0)
    check(status("first") == 204)
    check(status("first") == 204)
    check(status("first", signed = false) == 401)
    check(status("retry") == 500)
    check(status("retry") == 204)

    val running = CompletableFuture.supplyAsync { status("slow") }
    try {
        check(entered.await(10, TimeUnit.SECONDS))
        check(status("slow", receiver = endpoint()) == 503)
    } finally {
        release.countDown()
    }
    check(running.get(10, TimeUnit.SECONDS) == 204)
    check(status("slow") == 204)
    check(status("first", receiver = endpoint()) == 204)
    check(calls.get() == 4)
    println("PASS request verification, routing, body limit, retries, and persistent deduplication")
}

internal fun sign(secret: ByteArray, id: String, body: ByteArray): String {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(SecretKeySpec(secret, "HmacSHA256"))
    val digest = mac.doFinal(id.toByteArray() + byteArrayOf(10) + body)
    return "sha256=" + HexFormat.of().formatHex(digest)
}

private fun testWebhookPersistence() = withTempDirectory { directory ->
    val secret = "archive-test".toByteArray()
    val body = byteArrayOf(0, -1, 10)
    val saved = mutableMapOf<String, ByteArray>()
    val handled = mutableListOf<String>()
    var databaseAvailable = false
    var archiveAttempts = 0
    val archive = WebhookArchive { vendor, delivery ->
        check(vendor == "enabled")
        archiveAttempts++
        check(databaseAvailable)
        saved.putIfAbsent(delivery.id, delivery.body.copyOf())
    }
    fun endpoint(vendorName: String, enabled: Boolean, storage: WebhookArchive?) =
        WebhookEndpoint(object : WebhookHandler {
            override val vendor = vendorName
            override val persistWebhooks = enabled

            override fun handle(delivery: Delivery) {
                if (enabled) check(saved.getValue(delivery.id).contentEquals(body))
                handled += delivery.id
                delivery.body.fill(0)
                if (delivery.id == "handler-retry" && handled.count { it == delivery.id } == 1) {
                    error("handler unavailable")
                }
            }
        }, secret, directory, storage)

    fun send(receiver: WebhookEndpoint, id: String, valid: Boolean = true) = receiver.receive(
        "POST", receiver.route, id, if (valid) sign(secret, id, body) else null, body.inputStream()
    )

    check(runCatching { endpoint("missing", true, null) }.exceptionOrNull() is IllegalArgumentException)
    check(send(endpoint("disabled", false, archive), "ignored") == 204)
    val receiver = endpoint("enabled", true, archive)
    check(send(receiver, "invalid", valid = false) == 401)
    check(archiveAttempts == 0)
    check(send(receiver, "database-retry") == 503)
    check(handled == listOf("ignored"))
    databaseAvailable = true
    check(send(receiver, "database-retry") == 204)
    check(send(receiver, "database-retry") == 204)
    check(archiveAttempts == 2)
    check(send(receiver, "handler-retry") == 500)
    check(saved.getValue("handler-retry").contentEquals(body))
    check(send(receiver, "handler-retry") == 204)
    check(saved.keys == setOf("database-retry", "handler-retry"))
    println("PASS opt-in persistence, authentication, duplicate handling, and write-failure retries")
}
