package webhooks

import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.WRITE
import java.security.MessageDigest
import java.util.HexFormat
import java.util.concurrent.ConcurrentHashMap

/** Stores receipts on disk and coordinates claims using filesystem locks. */
class FileDeliveryReceipts(private val root: Path) : DeliveryReceipts {
    private val directories = ConcurrentHashMap<String, Path>()

    override fun process(vendor: String, deliveryId: String, handle: () -> Unit): ReceiptResult {
        require(vendorName.matches(vendor))
        val directory = directories.computeIfAbsent(vendor) {
            Files.createDirectories(root.resolve(it)).toRealPath()
        }
        val digest = MessageDigest.getInstance("SHA-256").digest(deliveryId.toByteArray())
        val key = HexFormat.of().formatHex(digest)
        val receipt = directory.resolve("$key.done")
        if (Files.exists(receipt)) return ReceiptResult.DUPLICATE

        val lockPath = directory.resolve("$key.lock")
        if (!activeLocks.add(lockPath)) return ReceiptResult.BUSY
        try {
            // Keep the lock file so processes always lock the same inode for a given ID.
            FileChannel.open(lockPath, CREATE, WRITE).use { channel ->
                val lock = channel.tryLock() ?: return ReceiptResult.BUSY
                lock.use {
                    if (Files.exists(receipt)) return ReceiptResult.DUPLICATE
                    handle()
                    Files.createFile(receipt)
                    return ReceiptResult.PROCESSED
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
