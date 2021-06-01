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
