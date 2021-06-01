package org.example.concepts


/**
 * Represents a read operation.
 *
 * Read operations run again state T, returning a set of [Cardinality.ZERO2MANY] values without changing T
 * (semantically).
 *
 * @property resultSet An approximation of the cardinality of the result set. E.g.,
 * - Reading from the structure returned by [emptyList] will always return zero elements. Therefore, all read operations
 * from that implementation (except for size), should be marked with [Cardinality.ZERO].
 * - However, and building upon the example above, [List.size] always returns a single element (which represents the
 * number of elements in the list). Therefore, the result set of [List.size] is [Cardinality.ONE].
 */
annotation class Read(val resultSet: Cardinality)
