import java.io.IOException
import java.net.InetSocketAddress
import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE_NEW
import java.nio.file.StandardOpenOption.WRITE
import java.sql.SQLException
import java.util.Locale
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess
import webhooks.PostgresWebhookArchive
import webhooks.PostgresWebhookJobs
import webhooks.ProcessingMode
import webhooks.WebhookHandler
import webhooks.postgresConnectionFactory
import webhooks.startWebhookServer
import webhooks.vendorName

fun main(args: Array<String>) {
    val result = try {
        runCli(args, Path.of("."), ::println)
    } catch (failure: IOException) {
        System.err.println(failure.message)
        1
    }
    exitProcess(result)
}

internal fun runCli(args: Array<String>, root: Path, output: (String) -> Unit): Int {
    val command = args.firstOrNull()
    val validCommand = (command == "generate" && args.size == 2) ||
        (command == "serve" && args.size in 2..3)
    if (!validCommand) {
        output("Usage: ./webhook generate <vendor> | ./webhook serve <vendor> [port]")
        return 2
    }
    val vendor = args[1].lowercase(Locale.ROOT)
    if (!vendorName.matches(vendor)) {
        output("Use a vendor name starting with a letter, with up to 63 letters, digits, or hyphens.")
        return 2
    }
    if (command == "generate") return generateHandler(vendor, root, output)
    return serve(vendor, args.getOrElse(2) { "8080" }, root, output)
}

private fun generateHandler(vendor: String, root: Path, output: (String) -> Unit): Int {
    val namespace = namespace(vendor)
    val target = root.resolve("src/webhooks/$namespace/Handler.kt")
    Files.createDirectories(target.parent)
    try {
        Files.writeString(target, """
            package webhooks.${if (namespace in kotlinKeywords) "`$namespace`" else namespace}

            import webhooks.Delivery
            import webhooks.WebhookHandler

            class Handler : WebhookHandler {
                override val vendor = "$vendor"
                override val persistWebhooks = false

                override fun handle(delivery: Delivery) {
                    // Replace this with vendor-specific handling.
                    println("$vendor handled delivery ${'$'}{delivery.id}")
                }
            }
        """.trimIndent() + "\n", CREATE_NEW, WRITE)
    } catch (_: FileAlreadyExistsException) {
        output("Handler already exists at $target")
        return 1
    }
    output("Generated $target for POST /webhooks/$vendor")
    return 0
}

internal fun loadHandler(vendor: String): WebhookHandler {
    require(vendorName.matches(vendor))
    val type = Class.forName("webhooks.${namespace(vendor)}.Handler")
    return type.asSubclass(WebhookHandler::class.java).getDeclaredConstructor().newInstance().also {
        require(it.vendor == vendor) { "Generated handler vendor must match $vendor." }
    }
}

private fun serve(vendor: String, portArgument: String, root: Path, output: (String) -> Unit): Int {
    val port = portArgument.toIntOrNull()
    if (port == null || port !in 1..65535) {
        output("Use a port between 1 and 65535.")
        return 2
    }
    val handler = try {
        loadHandler(vendor)
    } catch (_: ClassNotFoundException) {
        output("Run ./webhook generate $vendor to create the handler first.")
        return 1
    }
    val secret = System.getenv("WEBHOOK_SECRET")
    if (secret.isNullOrBlank()) {
        output("Set WEBHOOK_SECRET before starting the server.")
        return 2
    }
    val dataDirectory = root.resolve(System.getenv("WEBHOOK_DATA_DIR") ?: ".webhook-data")
    val async = handler.processingMode != ProcessingMode.SYNC
    if (handler.processingMode == ProcessingMode.ASYNC_ARCHIVE && !handler.persistWebhooks) {
        output("ASYNC_ARCHIVE requires persistWebhooks = true.")
        return 2
    }
    var archive: PostgresWebhookArchive? = null
    var jobs: PostgresWebhookJobs? = null
    if (async || handler.persistWebhooks) {
        val url = System.getenv("WEBHOOK_DATABASE_URL")
        if (url == null || !url.startsWith("jdbc:postgresql:")) {
            output("Set WEBHOOK_DATABASE_URL to a PostgreSQL JDBC URL for archiving or async processing.")
            return 2
        }
        try {
            Class.forName("org.postgresql.Driver")
        } catch (_: ClassNotFoundException) {
            output("Set WEBHOOK_JDBC_JAR to the PostgreSQL JDBC driver JAR.")
            return 2
        }
        val connect = postgresConnectionFactory(
            url, System.getenv("WEBHOOK_DATABASE_USER"), System.getenv("WEBHOOK_DATABASE_PASSWORD")
        )
        try {
            if (async) {
                jobs = PostgresWebhookJobs(connect).also { it.verifySchema() }
            } else {
                archive = PostgresWebhookArchive(connect).also { it.verifySchema() }
            }
        } catch (failure: SQLException) {
            output("Cannot access webhook storage (SQLSTATE ${failure.sqlState}). Check the connection and apply the database migrations in db/.")
            return 1
        }
    }
    val server = startWebhookServer(
        InetSocketAddress("127.0.0.1", port), handler, secret.toByteArray(), dataDirectory, archive, jobs
    )
    Runtime.getRuntime().addShutdownHook(Thread { server.close() })
    output("Listening for POST http://127.0.0.1:${server.port}/webhooks/$vendor")
    CountDownLatch(1).await()
    return 0
}

private fun namespace(vendor: String) = vendor.replace('-', '_')
private val kotlinKeywords = setOf(
    "as", "break", "class", "continue", "do", "else", "false", "for", "fun", "if", "in",
    "interface", "is", "null", "object", "package", "return", "super", "this", "throw",
    "true", "try", "typealias", "typeof", "val", "var", "when", "while"
)
