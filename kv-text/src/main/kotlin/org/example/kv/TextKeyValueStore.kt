package org.example.kv

import org.example.log.LineWithOffset
import org.example.log.Log

class TextKeyValueStore(private val log: Log):
        SeekableKeyValueStore {

    override fun put(key: String, value: String) {
        log.append("$key,$value")
    }

    override fun putAll(entries: Map<String, String>) {
        log.appendAll(entries.map { "${it.key},${it.value}" })
    }

    override fun putAndGetOffset(key: String, value: String): Long = log.append("${key},${value}")

    override fun get(key: String, offset: Long): String? = log.useLines(offset) { it.findLastKey(key) }

    override fun get(key: String): String? = log.useLines { it.findLastKey(key) }

    override fun getWithOffset(key: String): Pair<Long, String>? = log.useLinesWithOffset { it.findLastKey(key) }

    private fun Sequence<String>.findLastKey(key: String): String? = this
            .map { kvLine.split(it, 2) }
            .findLast { key == it[0] }?.get(1)

    private fun Sequence<LineWithOffset>.findLastKey(key: String) : Pair<Long, String>? = this
            .map { Pair(it.offset, kvLine.split(it.line, 2)) }
            .findLast { key == it.second[0] }?.let { Pair(it.first, it.second[1]) }

    companion object {
        val kvLine = Regex(",")
    }

}
