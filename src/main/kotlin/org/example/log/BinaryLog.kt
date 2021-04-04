package org.example.log

import java.io.BufferedOutputStream
import java.io.OutputStream
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Files.*
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.util.*
import java.util.zip.CRC32

private class BinaryLog(private val path: Path): Log<ByteArray> {

    private var mutableSize = path.size()

    override val size: Long
        get() = mutableSize

    override fun append(entry: ByteArray): Long {

        val offset = size

        path.writeOnly { it.writeEntry(entry) }

        mutableSize += headerSize + entry.size

        return offset
    }

    override fun appendAll(entries: Sequence<ByteArray>): Sequence<Long> {

        if (entries.none()) {
            return emptySequence()
        }

        val offsets: MutableList<Long> = LinkedList()
        offsets.add(size)

        path.writeOnly { stream ->
            entries.forEach {
                stream.writeEntry(it)
                val last = offsets.last()
                offsets.add(last + headerSize + it.size)
            }
        }

        mutableSize = offsets.last()

        return sequenceOf(0L) + offsets.subList(0, offsets.size - 1)
    }

    override fun <R> useEntries(offset: Long, block: (Sequence<ByteArray>) -> R): R = when {
        path.size() == 0L -> block(emptySequence())
        else -> path.readOnly {
            if (offset > 0) {
                it.seek(offset)
            }
            block(EntryIterator(it).asSequence())
        }
    }

    override fun <R> useEntriesWithOffset(offset: Long, block: (Sequence<EntryWithOffset<ByteArray>>) -> R): R = when {
        path.size() == 0L -> block(emptySequence())
        else -> path.readOnly {
            if (offset > 0) {
                it.seek(offset)
            }
            block(EntryWithOffsetIterator(it).asSequence())
        }
    }

    override fun clear() { delete(path) }

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

private fun <T> Path.readOnly(block: (RandomAccessFile) -> T): T = RandomAccessFile(toFile(), "r")
        .use(block)

private fun <T> Path.writeOnly(block: (OutputStream) -> T): T = BufferedOutputStream(newOutputStream(this,
        CREATE, APPEND))
        .use(block)

private fun Path.size(): Long = when {
    isRegularFile(this) -> readOnly { it.length() }
    else -> 0L
}

class BinaryLogFactory: LogFactory<ByteArray> {

    override fun create(logPath: Path): Log<ByteArray> = BinaryLog(logPath)

}
