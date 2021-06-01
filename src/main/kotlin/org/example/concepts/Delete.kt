package org.example.concepts

/**
 * Marks a method as a delete operation.
 *
 * Delete operations are applied to a set object such that the result object will contain a subset of entities, compared
 * to the original set.
 *
 * @property resultState Indicates the expected cardinality of the operation in regards to the entities removed upon
 * deletion. E.g.,:
 * - A "clear" operation will always leave set with zero elements. Therefore, it should be marked with
 *  [Cardinality.ZERO2MANY].
 * - An operation such as [MutableMap.remove], which removes an entry based on its key, will either remove an entry if
 *  it exists, or no entries if no entry with the provided key exists. Therefore, it should be marked with
 *  [Cardinality.ZERO2ONE].
 */
annotation class Delete(val resultState: Cardinality)
