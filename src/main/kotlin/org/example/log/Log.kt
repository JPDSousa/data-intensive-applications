package org.example.log

import java.nio.file.Path

interface Log<T> {

    fun append(entry: T): Long

    fun appendAll(entries: Sequence<T>): Sequence<Long>

    fun <R> useEntries(offset: Long = 0, block: (Sequence<T>) -> R): R

    fun <R> useEntriesWithOffset(offset: Long = 0, block: (Sequence<EntryWithOffset<T>>) -> R): R

    fun size(): Long

    fun clear()

}

data class EntryWithOffset<T>(val offset: Long, val entry: T)

interface LogFactory<T> {

    fun create(logPath: Path): Log<T>

}
