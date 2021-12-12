package org.example.log

import org.example.concepts.*
import org.example.concepts.Cardinality.ONE
import org.example.concepts.Cardinality.ZERO2MANY
import org.example.trash.Trash
import java.nio.file.Path

/**
 * An append-only data structure.
 *
 * Logs are broken down by entries.
 *
 * As an [AppendMixin], the [Long] type refers to the start offset in which a given entry is appended.
 */
interface Log<T>: AppendMixin<T, Long>, SizeMixin, SerializationMixin {

    @Read(ZERO2MANY) fun <R> useEntries(offset: Long = 0, block: (Sequence<T>) -> R): R

    @Read(ZERO2MANY) fun <R> useEntriesWithOffset(offset: Long = 0, block: (Sequence<EntryWithOffset<T>>) -> R): R

    @Read(ONE) val lastOffset: Long

    override val size: Int
        get() = useEntries { it.count() }
}

data class EntryWithOffset<T>(val offset: Long, val entry: T)

@Factory(Log::class)
interface LogFactory<T, L: Log<T>> {

    val trash: Trash<L>

    fun create(logPath: Path): Log<T>

}

typealias LogFactoryB<T> = LogFactory<T, Log<T>>
typealias EntryLogFactory<K, V> = LogFactoryB<Map.Entry<K, V>>

