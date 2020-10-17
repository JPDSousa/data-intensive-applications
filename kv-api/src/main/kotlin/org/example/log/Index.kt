package org.example.log

interface Index {

    fun putOffset(key: String, offset: Long)

    fun putAllOffsets(pairs: Iterable<Pair<String, Long>>) {
        pairs.forEach {
            putOffset(it.first, it.second)
        }
    }

    fun getOffset(key: String): Long?

}
