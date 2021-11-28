package org.example.concepts

/**
 * Represents a write operation.
 */
annotation class Write(val type: WriteTypes, val effect: Cardinality)

/**
 * The available write types.
 */
enum class WriteTypes {

    /**
     * Represents a sequential write operations. Sequential writes are generally much more efficient than RANDOM writes,
     * as they don't require moving seeking the position to write in the disk.
     */
    SEQUENTIAL,

    /**
     * Represents a write operation in an arbitrary position. Random writes generally require moving the data structure
     * cursor to the writing position.
     */
    RANDOM
}

interface ClearMixin {

    @Delete(Cardinality.ZERO)
    fun clear()
}

fun <K, V> MutableMap<K, V>.asClearMixin(): ClearMixin {

    val map = this

    return object: ClearMixin {

        override fun clear() {
            map.clear()
        }

    }
}

interface AppendMixin<T, R> {

    /**
     * Appends [entry] to the end of the data structure.
     */
    @Write(WriteTypes.SEQUENTIAL, Cardinality.ONE) fun append(entry: T): R

    /**
     * Appends multiple *entries* to the end of the data structure.
     */
    @Write(WriteTypes.SEQUENTIAL, Cardinality.ZERO2MANY) fun appendAll(entries: Sequence<T>): Sequence<R>

}