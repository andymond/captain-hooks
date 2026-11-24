package webhooks

import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.WRITE
import java.security.MessageDigest
import java.util.HexFormat
import java.util.concurrent.ConcurrentHashMap

internal class DeliveryStore(directory: Path) {
    enum class Result { PROCESSED, DUPLICATE, BUSY }

    private val directory = Files.createDirectories(directory).toRealPath()

    fun process(id: String, handle: () -> Unit): Result {
        val key = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(id.toByteArray()))
        val receipt = directory.resolve("$key.done")
        if (Files.exists(receipt)) return Result.DUPLICATE

        val lockPath = directory.resolve("$key.lock")
        if (!activeLocks.add(lockPath)) return Result.BUSY
        try {
            // Keep the lock file so processes always lock the same inode for a given ID.
            FileChannel.open(lockPath, CREATE, WRITE).use { channel ->
                val lock = channel.tryLock() ?: return Result.BUSY
                lock.use {
                    if (Files.exists(receipt)) return Result.DUPLICATE
                    handle()
                    Files.createFile(receipt)
                    return Result.PROCESSED
                }
            }
        } finally {
            activeLocks.remove(lockPath)
        }
    }

    private companion object {
        // Avoid opening another channel on the same lock file within this JVM.
        val activeLocks = ConcurrentHashMap.newKeySet<Path>()
    }
}
