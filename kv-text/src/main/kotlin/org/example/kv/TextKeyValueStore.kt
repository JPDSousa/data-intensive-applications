package org.example.kv

import org.example.log.Index
import org.example.log.EntryWithOffset
import org.example.log.Log

class TextKeyValueStore(private val index: Index, internal val log: Log): KeyValueStore {

    override fun put(key: String, value: String) {
        val offset = putAndGetOffset(key, value)
        this.index.putOffset(key, offset)
    }

    override fun putAll(entries: Map<String, String>) {
        if (entries.isNotEmpty()) {
            val content = ArrayList<String>(entries.size)
            val keys = ArrayList<String>(entries.size)

            for (entry in entries) {
                content.add("${entry.key},${entry.value}")
                keys.add(entry.key)
            }

            val offsets = log.appendAll(content)

            index.putAllOffsets(keys.zip(offsets))
        }
    }

    private fun putAndGetOffset(key: String, value: String) = log.append("${key},${value}")

    private fun get(key: String, offset: Long): String? = log.useEntries(offset) { it.findLastKey(key) }

    private fun getRaw(key: String, nullifyTombstone: Boolean): String? {

        val offset = index.getOffset(key)

        if (offset == tombstoneIndex) {
            return if (nullifyTombstone) null else tombstone
        }

        if (offset != null) {
            return get(key, offset)
        }

        return getWithOffset(key)
                ?.also { index.putOffset(it.second, it.first) }
                ?.second
    }

    internal fun getWithTombstone(key: String) = getRaw(key, false)

    override fun get(key: String) = getRaw(key, true)

    override fun delete(key: String) {
        log.append("$key,$tombstone")
        index.putOffset(key, tombstoneIndex)
    }

    override fun clear() = log.clear()

    internal fun <T> useEntries(offset: Long = 0, block: (Sequence<Pair<String, String>>) -> T): T
            = log.useEntries(offset) {
        block(it.map { line ->
            val split = line.toEntry()
            Pair(split[0], split[1])
        })
    }

    private fun getWithOffset(key: String): Pair<Long, String>? = log.useEntriesWithOffset { it.findLastKey(key) }

    private fun Sequence<String>.findLastKey(key: String): String? = this
            .map { it.toEntry() }
            .findLast { key == it[0] }?.get(1)

    private fun Sequence<EntryWithOffset>.findLastKey(key: String) : Pair<Long, String>? = this
            .map { Pair(it.offset, it.line.toEntry()) }
            .findLast { key == it.second[0] }?.let { Pair(it.first, it.second[1]) }

    private fun String.toEntry() = kvLine.split(this, 2)

    companion object {
        internal val kvLine = Regex(",")
        internal const val tombstone = "tombstone"
        private const val tombstoneIndex = -1L
    }

}
