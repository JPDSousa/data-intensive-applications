package org.example.concepts

/**
 * Metadata for the expected cardinality in regards to the operation effect.
 *
 * See usages of this type to understand how it can be applied to different types of operations.
 */
enum class Cardinality {

    /**
     * An empty result set.
     */
    ZERO,

    /**
     *
     */
    ZERO2ONE,

    /**
     * A single-entity result.
     */
    ONE,

    /**
     * A result set with 0..N entities.
     */
    ZERO2MANY,

    /**
     * A result set with 1..N entities.
     */
    ONE2MANY
}