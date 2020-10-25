package org.example.log

import java.io.RandomAccessFile
import java.lang.System.lineSeparator
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files.*
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import kotlin.streams.asSequence
import kotlin.streams.asStream

// TODO if the write ratio is too high, use a buffered writer to avoid I/O calls. The buffer is then flushed upon a read
class LineLog(private val path: Path, private val charset: Charset = UTF_8): Log<String> {

    private var size = if (exists(path)) size(path) else 0L

    override fun append(entry: String): Long {

        val offset = size
        write(path, listOf(entry), charset, CREATE, APPEND)
        size += entry.byteLength() + lineSeparator().byteLength()

        return offset
    }

    override fun appendAll(entries: Collection<String>): Collection<Long> {

        if (entries.isEmpty()) {
            return listOf()
        }

        val offsets = entries.runningFold(size, { acc, line ->
            acc + line.byteLength()
        })
        write(path, entries, charset, CREATE, APPEND)

        size = offsets.last()

        return listOf(0L) + offsets.subList(0, offsets.size - 1)
    }

    override fun <T> useEntries(offset: Long, block: (Sequence<String>) -> T): T {

        if(notExists(path)) {
            return block(emptySequence())
        }

        if (offset == 0L) {
            return lines(path, charset)
                    .use { block(it.asSequence()) }
        }

        return path.randomAccessReadOnly()
                .apply { seek(offset) }
                .generateStream()
                .use { block(it.asSequence()) }
    }

    override fun <T> useEntriesWithOffset(offset: Long, block: (Sequence<EntryWithOffset<String>>) -> T): T {

        if(notExists(path)) {
            return block(emptySequence())
        }

        return path.randomAccessReadOnly()
                .apply { seek(offset) }
                .generateSequenceWithOffset()
                .use { block(it.asSequence()) }
    }

    override fun size(): Long = size

    override fun clear() { delete(path) }

    private fun String.byteLength() = this.toByteArray(charset).size

    private fun Path.randomAccessReadOnly() : RandomAccessFile = RandomAccessFile(this.toFile(), "r")
}

internal fun RandomAccessFile.generateStream() = generateSequence { readLine() }
        .asStream()
        .onClose { this.close() }


private fun RandomAccessFile.generateSequenceWithOffset() = generateSequence {
    val pointer = filePointer
    val line = readLine()

    line?.let { EntryWithOffset(pointer, it) }
}.asStream().onClose { this.close() }
