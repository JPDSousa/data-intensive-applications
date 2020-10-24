package org.example.log

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8

class StringEncoderLog(private val log: Log<ByteArray>, private val charset: Charset = UTF_8): Log<String> {

    override fun append(entry: String) = log.append(entry.encode())

    override fun appendAll(entries: Collection<String>) = log.appendAll(entries.map { it.encode() })

    override fun <R> useEntries(offset: Long, block: (Sequence<String>) -> R) = log.useEntries(offset) { sequence ->
        block(sequence.map { it.decode() })
    }

    override fun <R> useEntriesWithOffset(offset: Long, block: (Sequence<EntryWithOffset<String>>) -> R)
            = log.useEntriesWithOffset { sequence ->
                block(sequence.map {
                    EntryWithOffset(it.offset, it.entry.decode())
                })
            }

    override fun size() = log.size()

    override fun clear() = log.clear()

    private fun ByteArray.decode() = String(this, charset)

    private fun String.encode() = this.toByteArray(charset)

}
