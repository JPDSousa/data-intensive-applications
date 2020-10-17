package org.example.log

interface Log {

    fun append(line: String): Long

    fun appendAll(lines: Collection<String>): Collection<Long>

    fun <T> useLines(offset: Long = 0, block: (Sequence<String>) -> T): T

    fun <T> useLinesWithOffset(offset: Long = 0, block: (Sequence<LineWithOffset>) -> T): T

    fun size(): Long

}

data class LineWithOffset(val offset: Long, val line: String)
