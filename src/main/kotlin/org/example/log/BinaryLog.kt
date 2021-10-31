package org.example.log

import org.example.readOnly
import org.example.size
import org.koin.core.qualifier.named
import java.io.BufferedOutputStream
import java.io.OutputStream
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Files.delete
import java.nio.file.Files.newOutputStream
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.util.zip.CRC32

val binaryLogQ = named("binarylog")

private class BinaryLog(private val path: Path,
                        override var lastOffset: Long,
                        override var byteLength: Long): Log<ByteArray> {

    override fun append(entry: ByteArray): Long {

        lastOffset = byteLength

        path.writeOnly { it.writeEntry(entry) }

        byteLength += headerSize + entry.size

        return lastOffset
    }

    override fun appendAll(entries: Sequence<ByteArray>): Sequence<Long> {

        if (entries.none()) {
            return emptySequence()
        }

        return path.writeOnly { stream ->
            entries.map { entry ->
                // 'size' is stale at this point, containing the lastOffset
                lastOffset = byteLength

                stream.writeEntry(entry)

                byteLength += headerSize + entry.size
                return@map lastOffset
            }.toList().asSequence()
        }
    }

    override fun <R> useEntries(offset: Long, block: (Sequence<ByteArray>) -> R): R = when (byteLength) {
        0L -> block(emptySequence())
        else -> path.readOnly {
            if (offset > 0) {
                it.seek(offset)
            }
            block(EntryIterator(it).asSequence())
        }
    }

    override fun <R> useEntriesWithOffset(
        offset: Long,
        block: (Sequence<EntryWithOffset<ByteArray>>) -> R
    ): R = when(path.size() == 0L) {
        true -> block(emptySequence())
        false -> path.readOnly {
            if (offset > 0) {
                it.seek(offset)
            }
            block(EntryWithOffsetIterator(it).asSequence())
        }
    }

    override fun clear() {
        delete(path)
        byteLength = 0L
        lastOffset = 0L
    }

    companion object {

        // entrySize + checksum
        const val headerSize = Int.SIZE_BYTES + Long.SIZE_BYTES
    }

}

private abstract class AbstractEntryIterator<T>(private val raf: RandomAccessFile): Iterator<T> {

    override fun hasNext(): Boolean = raf.filePointer < raf.length()
}

private class EntryIterator(private val raf: RandomAccessFile): AbstractEntryIterator<ByteArray>(raf) {

    override fun next() = raf.readEntry()

}

private class EntryWithOffsetIterator(private val raf: RandomAccessFile):
        AbstractEntryIterator<EntryWithOffset<ByteArray>>(raf) {

    override fun next(): EntryWithOffset<ByteArray> {
        val pointer = raf.filePointer
        return EntryWithOffset(pointer, raf.readEntry())
    }

}

private fun OutputStream.writeEntry(entry: ByteArray) {

    val header = ByteBuffer.allocate(BinaryLog.headerSize)

    val checksum = CRC32()
    checksum.update(entry)
    header.putLong(checksum.value)

    header.putInt(entry.size)
    write(header.array())
    write(entry)
}

private fun RandomAccessFile.readEntry(): ByteArray {

    val checksum = readLong()

    val size = readInt()
    val content = ByteArray(size)
    read(content)

    val hash = CRC32()
    hash.update(content)
    require(checksum == hash.value) { "Checksum doesn't match for entry $content (expected: $checksum, got: $hash)" }

    return content
}

private fun <T> Path.writeOnly(block: (OutputStream) -> T): T = BufferedOutputStream(
    newOutputStream(this, CREATE, APPEND))
    .use(block)

class BinaryLogFactory: LogFactory<ByteArray> {

    override fun create(logPath: Path): Log<ByteArray> {

        val size = logPath.size()
        val lastOffset = when (size) {
            0L -> 0L
            else -> logPath.readOnly {
                // will process the entire file, unfortunately
                EntryWithOffsetIterator(it).asSequence()
                    .lastOrNull()
                    ?.offset
                    ?: 0L
            }
        }

        return BinaryLog(logPath, lastOffset, size)
    }

}
