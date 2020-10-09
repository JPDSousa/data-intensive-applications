package org.example.log

import java.io.RandomAccessFile
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.file.Files.*
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import kotlin.streams.asSequence

class SingleFileLog(private val path: Path, private val charset: Charset = UTF_8): Log {

    var len = if (exists(path)) size(path) else 0L

    override fun append(line: String): Long {

        val offset = len
        write(path, listOf(line), charset, CREATE, APPEND)
        len += line.byteLength()

        return offset
    }

    override fun appendAll(lines: List<String>): List<Long> {

        if (lines.isEmpty()) {
            return listOf()
        }

        val offsets = lines.runningFold(len, { acc, line ->
            acc + line.byteLength()
        })
        write(path, lines, charset, CREATE, APPEND)

        len = offsets.last()

        return listOf(0L) + offsets.subList(0, offsets.size - 1)
    }

    override fun lines(offset: Long): Sequence<String> {

        if (offset == 0L) {
            return lines(path, charset).asSequence()
        }

        return path.randomAccessReadOnly()
                .apply { seek(offset) }
                .generateSequence()
    }

    override fun linesWithOffset(offset: Long): Sequence<Pair<Long, String>> = path.randomAccessReadOnly()
            .apply { seek(offset) }
            .generateSequenceWithOffset()

    private fun RandomAccessFile.generateSequence() = generateSequence {
        val line = readLine()

        if (line == null) {
            close()
        }

        line
    }

    private fun RandomAccessFile.generateSequenceWithOffset() = generateSequence {
        val pointer = filePointer
        val line = readLine()

        if (line == null) {
            close()
        }

        line?.let { Pair(pointer, it) }
    }

    private fun String.byteLength() = this.toByteArray(charset).size

    private fun Path.randomAccessReadOnly() : RandomAccessFile = RandomAccessFile(this.toFile(), "r")
}
