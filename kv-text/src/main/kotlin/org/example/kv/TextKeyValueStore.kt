package org.example.kv

import org.example.log.Log

class TextKeyValueStore(private val log: Log):
        SeekableKeyValueStore {

    private val kvLine = Regex(",")

    override fun put(key: String, value: String) {
        log.append("$key,$value")
    }

    override fun putAll(entries: Map<String, String>) {
        log.appendAll(entries.map { "${it.key},${it.value}" })
    }

    override fun putAndGetOffset(key: String, value: String): Long = log.append("${key},${value}")

    override fun get(key: String, offset: Long): String? = log.lines(offset)
            .findLastKey(key)

    override fun get(key: String): String? = log.lines()
            .findLastKey(key)

    override fun getWithOffset(key: String): Pair<Long, String>? = log.linesWithOffset()
            .findLastKey(key)

    private fun Sequence<String>.findLastKey(key: String): String? = this
            .map { kvLine.split(it, 2) }
            .findLast { key == it[0] }?.get(1)

    private fun Sequence<Pair<Long, String>>.findLastKey(key: String) : Pair<Long, String>? = this
            .onEach { print(it) }
            .map { Pair(it.first, kvLine.split(it.second, 2)) }
            .findLast { key == it.second[0] }?.let { Pair(it.first, it.second[1]) }

}
