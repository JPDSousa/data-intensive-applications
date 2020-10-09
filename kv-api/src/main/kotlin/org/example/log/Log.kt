package org.example.log

interface Log {

    fun append(line: String): Long

    fun appendAll(lines: List<String>): List<Long>

    fun lines(offset: Long = 0): Sequence<String>

    fun linesWithOffset(offset: Long = 0): Sequence<Pair<Long, String>>

}
