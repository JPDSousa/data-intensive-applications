package org.example.log

import org.example.concepts.SerializationMixin
import org.example.encoder.Encoder
import org.example.trash.Trash
import org.koin.core.qualifier.named
import java.nio.file.Path

val logEncoderQ = named(LogEncoderFactory.LogEncoder::class.qualifiedName!!)

class LogEncoderFactory<S, T>(private val innerFactory: LogFactoryB<T>,
                              private val encoder: Encoder<S, T>
): LogFactory<S, LogEncoderFactory<S, T>.LogEncoder> {

    override fun create(logPath: Path): Log<S> = LogEncoder(innerFactory.create(logPath), encoder)

    override val trash: Trash<LogEncoder> = EncodedTrash()

    inner class LogEncoder(
        val log: Log<T>,
        private val encoder: Encoder<S, T>
    ): Log<S>, SerializationMixin by log {

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

    private inner class EncodedTrash: Trash<LogEncoder> {

        override fun mark(deleteMe: LogEncoder) {
            innerFactory.trash.mark(deleteMe.log)
        }

    }
}
