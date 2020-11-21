package org.example.log

class LogEncoder<S, T>(private val log: Log<T>,
                       private val encoder: (S) -> T,
                       private val decoder: (T) -> S): Log<S> {

    override fun append(entry: S) = log.append(encoder(entry))

    override fun appendAll(entries: Collection<S>) = log.appendAll(entries.map { encoder(it) })

    override fun <R> useEntries(offset: Long, block: (Sequence<S>) -> R) = log.useEntries(offset) { sequence ->
        block(sequence.map { decoder(it) })
    }

    override fun <R> useEntriesWithOffset(offset: Long, block: (Sequence<EntryWithOffset<S>>) -> R)
            = log.useEntriesWithOffset { sequence ->
        block(sequence.map { EntryWithOffset(it.offset, decoder(it.entry)) })
    }

    override fun size() = log.size()

    override fun clear() = log.clear()
}
