package org.example.log

import org.example.concepts.Cardinality.*
import org.example.concepts.Delete
import org.example.concepts.Factory
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
     *
     * Returns the offset of the inserted entry. Offsets are non-negative values.
     */
    @Write(SEQUENTIAL, ONE) fun append(entry: T): Long

    /**
     * Appends multiple *entries* to the end of the data structure.
     */
    @Write(SEQUENTIAL, ZERO2MANY) fun appendAll(entries: Sequence<T>): Sequence<Long>

    @Read(ZERO2MANY) fun <R> useEntries(offset: Long = 0, block: (Sequence<T>) -> R): R

    @Read(ZERO2MANY) fun <R> useEntriesWithOffset(offset: Long = 0, block: (Sequence<EntryWithOffset<T>>) -> R): R

    @Read(ONE) val size: Long

    @Delete(ZERO)
    fun clear()

}

data class EntryWithOffset<T>(val offset: Long, val entry: T)

@Factory(Log::class)
interface LogFactory<T> {

    fun create(logPath: Path): Log<T>

}
