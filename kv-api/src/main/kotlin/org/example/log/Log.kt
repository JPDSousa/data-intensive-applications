package org.example.log

interface Log {

    fun append(line: String): Long

    fun appendAll(lines: Collection<String>): Collection<Long>

    fun <T> useEntries(offset: Long = 0, block: (Sequence<String>) -> T): T

    fun <T> useEntriesWithOffset(offset: Long = 0, block: (Sequence<EntryWithOffset>) -> T): T

    fun size(): Long

    fun clear()

}

data class EntryWithOffset(val offset: Long, val line: String)
