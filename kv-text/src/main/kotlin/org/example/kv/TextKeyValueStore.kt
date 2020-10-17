package org.example.kv

import org.example.log.LineWithOffset
import org.example.log.Log

class TextKeyValueStore(val log: Log): KeyValueStore {

    private val index = mutableMapOf<String, Long>()

    override fun put(key: String, value: String) {
        index[key] = putAndGetOffset(key, value)
    }

    override fun putAll(entries: Map<String, String>) {
        if (entries.isNotEmpty()) {
            log.appendAll(entries.map { "${it.key},${it.value}" }).last()
        }
    }

    private fun putAndGetOffset(key: String, value: String) = log.append("${key},${value}")

    private fun get(key: String, offset: Long): String? = log.useLines(offset) { it.findLastKey(key) }

    override fun get(key: String): String? {

        val offset = index[key]

        if (offset != null) {
            return get(key, offset)
        }

        return getWithOffset(key)
                ?.also { index[it.second] = it.first }
                ?.second
    }

    internal fun <T> useEntries(offset: Long = 0, block: (Sequence<Pair<String, String>>) -> T): T
            = log.useLines(offset) {
        block(it.map { line ->
            val split = line.toEntry()
            Pair(split[0], split[1])
        })
    }

    private fun getWithOffset(key: String): Pair<Long, String>? = log.useLinesWithOffset { it.findLastKey(key) }

    private fun Sequence<String>.findLastKey(key: String): String? = this
            .map { it.toEntry() }
            .findLast { key == it[0] }?.get(1)

    private fun Sequence<LineWithOffset>.findLastKey(key: String) : Pair<Long, String>? = this
            .map { Pair(it.offset, it.line.toEntry()) }
            .findLast { key == it.second[0] }?.let { Pair(it.first, it.second[1]) }

    private fun String.toEntry() = kvLine.split(this, 2)

    companion object {
        internal val kvLine = Regex(",")
    }

}
