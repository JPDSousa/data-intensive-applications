package org.example.log

import java.io.BufferedOutputStream
import java.io.OutputStream
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Files.*
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE

class BinaryLog(private val path: Path): Log<ByteArray> {

    private var size = path.size()

    override fun append(entry: ByteArray): Long {

        val offset = size

        path.writeOnly { it.writeEntry(entry) }

        // header size
        size += 4 + entry.size

        return offset
    }

    override fun appendAll(entries: Collection<ByteArray>): Collection<Long> {

        if (entries.isEmpty()) {
            return emptyList()
        }

        val offsets = entries.runningFold(size, { acc, entry -> acc + 4 + entry.size })

        path.writeOnly { stream ->
            entries.forEach {
                stream.writeEntry(it)
            }
        }

        size = offsets.last()

        return listOf(0L) + offsets.subList(0, offsets.size - 1)
    }

    override fun <R> useEntries(offset: Long, block: (Sequence<ByteArray>) -> R): R {

        if(path.size() == 0L) {
            return block(emptySequence())
        }

        return path.readOnly {
            if (offset > 0) {
                it.seek(offset)
            }
            block(EntryIterator(it).asSequence())
        }
    }

    override fun <R> useEntriesWithOffset(offset: Long, block: (Sequence<EntryWithOffset<ByteArray>>) -> R): R {

        if(path.size() == 0L) {
            return block(emptySequence())
        }

        return path.readOnly {
            if (offset > 0) {
                it.seek(offset)
            }
            block(EntryWithOffsetIterator(it).asSequence())
        }
    }

    override fun size() = size

    override fun clear() { delete(path) }

}

private abstract class AbstractEntryIterator<T>(private val raf: RandomAccessFile): Iterator<T> {

    override fun hasNext(): Boolean = raf.filePointer < raf.length()
}

private class EntryIterator(private val raf: RandomAccessFile): AbstractEntryIterator<ByteArray>(raf) {

    override fun next(): ByteArray {
        val size = raf.readInt()
        val content = ByteArray(size)
        raf.read(content)
        return content
    }

}

private class EntryWithOffsetIterator(private val raf: RandomAccessFile):
        AbstractEntryIterator<EntryWithOffset<ByteArray>>(raf) {

    override fun next(): EntryWithOffset<ByteArray> {
        val pointer = raf.filePointer
        val size = raf.readInt()
        val content = ByteArray(size)
        raf.read(content)
        return EntryWithOffset(pointer, content)
    }

}

private fun OutputStream.writeEntry(entry: ByteArray) {
    val header = ByteBuffer.allocate(4)
    header.putInt(entry.size)
    write(header.array())
    write(entry)
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
