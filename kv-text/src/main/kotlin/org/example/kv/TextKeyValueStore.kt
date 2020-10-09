package org.example.kv

import java.io.RandomAccessFile
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.nio.file.Files.size
import java.nio.file.Files.write
import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import kotlin.streams.asSequence

class TextKeyValueStore(private val path: Path, private val charset: Charset = UTF_8): SeekableKeyValueStore {

    private val kvLine = Regex(",")

    private var offset = size(path)

    override fun put(key: String, value: String) = write(listOf("\n$key,$value"))

    override fun putAll(entries: Map<String, String>) = write(entries.map { "\n${it.key},${it.value}" })

    private fun write(entries: List<String>) {

        val nBytes = entries.map { it.toByteArray(charset).size }.sum()
        write(path, entries, charset, APPEND)
        offset += nBytes
    }

    override fun putAndGetOffset(key: String, value: String): Long {
        val offset = this.offset
        put(key, value)
        return offset;
    }

    override fun get(key: String, offset: Long): String? {
        val file = path.randomAccessReadOnly()

        return file.apply { seek(offset) }
                .let { generateSequence { it.readLine() } }
                .findLastKey(key)
                .also { file.close() }
    }

    override fun get(key: String): String? = Files.lines(path, charset)
            .asSequence()
            .findLastKey(key)

    override fun getWithOffset(key: String): Pair<Long, String>? {
        val file = path.randomAccessReadOnly()

        return file.let { generateSequence {
            val pointer = it.filePointer
            val line = it.readLine()
            return@generateSequence if (line == null) null else Pair(pointer, line)
        } }
                .findLastKey(key)
                .also { file.close() }
    }

    private fun Sequence<String>.findLastKey(key: String): String? = this
            .map { kvLine.split(it, 2) }
            .findLast { key == it[0] }?.get(1)

    private fun Sequence<Pair<Long, String>>.findLastKey(key: String) : Pair<Long, String>? = this
            .onEach { print(it) }
            .map { Pair(it.first, kvLine.split(it.second, 2)) }
            .findLast { key == it.second[0] }?.let { Pair(it.first, it.second[1]) }

    private fun Path.randomAccessReadOnly() : RandomAccessFile = RandomAccessFile(this.toFile(), "r")
}
