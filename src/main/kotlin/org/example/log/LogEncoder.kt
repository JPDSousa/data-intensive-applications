package org.example.log

import org.example.concepts.ClearMixin
import org.example.concepts.SerializationMixin
import org.example.encoder.Encoder
import org.koin.core.qualifier.named
import java.nio.file.Path

val logEncoderQ = named(LogEncoder::class.qualifiedName!!)

private class LogEncoder<S, T>(
    private val log: Log<T>,
    private val encoder: Encoder<S, T>
): Log<S>,
    SerializationMixin by log,
    ClearMixin by log {

    override fun append(entry: S) = log.append(encoder.encode(entry))

    override fun appendAll(entries: Sequence<S>) = log.appendAll(entries.map { encoder.encode(it) })

    override fun <R> useEntries(offset: Long, block: (Sequence<S>) -> R) = log.useEntries(offset) { sequence ->
        block(sequence.map { encoder.decode(it) })
    }

    override fun <R> useEntriesWithOffset(offset: Long, block: (Sequence<EntryWithOffset<S>>) -> R)
            = log.useEntriesWithOffset { sequence ->
        block(sequence.map { EntryWithOffset(it.offset, encoder.decode(it.entry)) })
    }

    override val lastOffset: Long
        get() = log.lastOffset
}

class LogEncoderFactory<S, T>(private val innerFactory: LogFactory<T>,
                              private val encoder: Encoder<S, T>): LogFactory<S> {

    override fun create(logPath: Path): Log<S> = LogEncoder(innerFactory.create(logPath), encoder)
}
