package org.example.log

import org.example.concepts.Delete
import org.example.concepts.Read
import org.example.concepts.Write
import org.example.concepts.WriteTypes.SEQUENTIAL
import java.nio.file.Path

/**
 * An append-only data structure.
 *
 * Logs are broken down by entries.
 */
interface Log<T> {

    /**
     * Appends *entry* to the end of the data structure.
     */
    @Write(SEQUENTIAL) fun append(entry: T): Long

    @Write(SEQUENTIAL) fun appendAll(entries: Sequence<T>): Sequence<Long>

    @Read fun <R> useEntries(offset: Long = 0, block: (Sequence<T>) -> R): R

    @Read fun <R> useEntriesWithOffset(offset: Long = 0, block: (Sequence<EntryWithOffset<T>>) -> R): R

    val size: Long

    @Delete
    fun clear()

}

data class EntryWithOffset<T>(val offset: Long, val entry: T)

interface LogFactory<T> {

    fun create(logPath: Path): Log<T>

}
