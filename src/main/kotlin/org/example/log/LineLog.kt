package org.example.log

import org.koin.core.qualifier.named
import java.io.RandomAccessFile
import java.lang.System.lineSeparator
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files.*
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.util.*
import java.util.zip.CRC32
import kotlin.streams.asSequence
import kotlin.streams.asStream

// TODO if the write ratio is too high, use a buffered writer to avoid I/O calls. The buffer is then flushed upon a read
private class LineLog(private val path: Path,
                      private val charset: Charset,
                      override var size: Long,
                      override var lastOffset: Long): Log<String> {

    override fun append(entry: String): Long {

        lastOffset = size

        val line = entry.prependHeader()
        write(path, listOf(line), charset, CREATE, APPEND)
        size += line.entrySize()

        return lastOffset
    }

    override fun appendAll(entries: Sequence<String>): Sequence<Long> {

        if (entries.none()) {
            return emptySequence()
        }

        val offsets: MutableList<Long> = LinkedList()
        offsets.add(size)

        entries.map { it.prependHeader() }
            .onEach {
                val last = offsets.last()
                offsets.add(last + it.entrySize())
            }.let { write(path, it.asIterable(), charset, CREATE, APPEND) }

        size = offsets.last()

        return offsets.subList(0, offsets.size - 1).asSequence()
    }

    override fun <T> useEntries(offset: Long, block: (Sequence<String>) -> T): T {

        if(notExists(path)) {
            return block(emptySequence())
        }

        if (offset == 0L) {
            return lines(path, charset)
                    .use { stream -> stream.asSequence().map { it.stripHeader() }.let(block) }
        }

        return path.randomAccessReadOnly()
                .apply { seek(offset) }
                // use buffer at this point
                .generateStream()
                .use { stream -> stream.asSequence().map { it.stripHeader() }.let(block) }
    }

    override fun <T> useEntriesWithOffset(offset: Long, block: (Sequence<EntryWithOffset<String>>) -> T): T {

        if(notExists(path)) {
            return block(emptySequence())
        }

        return path.randomAccessReadOnly()
                .apply { seek(offset) }
                .generateSequenceWithOffset()
                .use { stream -> stream.asSequence().map { it.stripHeader() }.let(block) }
    }

    override fun clear() {
        delete(path)
        size = 0L
        lastOffset = 0L
    }

    private fun String.byteLength() = toByteArray(charset).size

    private fun String.entrySize() = byteLength() + lineSeparator().byteLength()

    private fun String.prependHeader(): String {

        val checksum = CRC32()
        checksum.update(toByteArray(charset))

        return "${checksum.value}-$this"
    }

    private fun String.stripHeader(): String {

        val match = headerRegex.matchEntire(this)!!
        val checksum = match.destructured.component1().toLong()
        val content = match.destructured.component2()

        val hash = CRC32()
        hash.update(content.toByteArray(charset))
        require(hash.value == checksum) {
            "Checksum doesn't match for entry $content (expected: $checksum, got: $hash)"
        }

        return content
    }

    private fun EntryWithOffset<String>.stripHeader() = EntryWithOffset(offset, entry.stripHeader())

    companion object {
        private val headerRegex = Regex("(\\d+)-(.*)")
    }

}

private fun Path.randomAccessReadOnly() : RandomAccessFile = RandomAccessFile(this.toFile(), "r")

internal fun RandomAccessFile.generateStream() = generateSequence { readLine() }
        .asStream()
        .onClose { this.close() }


private fun RandomAccessFile.generateSequenceWithOffset() = generateSequence {
    val pointer = filePointer
    val line = readLine()

    line?.let { EntryWithOffset(pointer, it) }
}.asStream().onClose { this.close() }


class LineLogFactory: LogFactory<String> {

    override fun create(logPath: Path): Log<String> {

        val size = if (exists(logPath)) size(logPath) else 0L

        val lastOffset = when (size) {
            0L -> 0L
            else -> logPath.randomAccessReadOnly()
                .generateSequenceWithOffset()
                .asSequence()
                .last()
                .offset
        }

        return LineLog(logPath, UTF_8, size, lastOffset)
    }

    override fun toString(): String = "String Log Factory"
}

val lineLogQ = named("lineLog")
