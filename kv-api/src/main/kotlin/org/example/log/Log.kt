package org.example.log

interface Log<T> {

    fun append(entry: T): Long

    fun appendAll(entries: Collection<T>): Collection<Long>

    fun <R> useEntries(offset: Long = 0, block: (Sequence<T>) -> R): R

    fun <R> useEntriesWithOffset(offset: Long = 0, block: (Sequence<EntryWithOffset<T>>) -> R): R

    fun size(): Long

    fun clear()

}

data class EntryWithOffset<T>(val offset: Long, val entry: T)
