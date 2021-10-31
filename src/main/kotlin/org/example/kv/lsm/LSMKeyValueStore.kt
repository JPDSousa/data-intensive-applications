package org.example.kv.lsm

import kotlinx.coroutines.CoroutineDispatcher
import mu.KotlinLogging
import org.example.concepts.CompressMixin
import org.example.concepts.ImmutableDictionaryMixin
import org.example.kv.KeyValueStore
import org.example.possiblyArrayEquals
import org.example.recurrent.OpsBasedRecurrentJob
import org.example.recurrent.RecurrentJob

/**
 * Log Structured Merge key value store.
 *
 * This Key-value store design is based on a series of [Segment], which store the structure data, and are occasionally
 * compacted (see the definition of this operation in [compress]).
 */
// TODO should extend from LogKeyValueStore
interface LSMKeyValueStore<K, V>: KeyValueStore<K, V>, CompressMixin {

    /**
     * Rearranges the current list of segments so that:
     * - The resulting number of segments is less or equal than the original number of segments.
     * - No valuable data is lost. Only redundant (i.e., stale) data is deleted.
     * - The operation is idempotent. Compacting an LSM structure twice without updating the state in between operations
     *   will render the second compact operation useless.
     * - Compact operations without any effect on the underlying state may have a non-negligible computational cost.
     * - Some implementations may call compact internally (similarly to [java.io.Writer.flush]).
     */
    override fun compress()
}

private class RecurrentMergeDecorator<K, V>(
    private val decorated: LSMKeyValueStore<K, V>,
    private val recurrentJob: RecurrentJob
): LSMKeyValueStore<K, V>, ImmutableDictionaryMixin<K, V> by decorated {

    override fun put(key: K, value: V) {
        decorated[key] = value
        recurrentJob.registerOperation()
    }

    override fun delete(key: K) {
        decorated.delete(key)
        recurrentJob.registerOperation()
    }

    override fun clear() {
        decorated.clear()
        recurrentJob.reset()
    }

    override fun compress() {
        decorated.compress()
    }

}

private class SegmentedLSM<K, V>(private val segmentManager: SegmentManager<K, V>,
                                 private val tombstone: V): LSMKeyValueStore<K, V> {

    private val logger = KotlinLogging.logger {}

    private val closedSegments = segmentManager.loadClosedSegments()
    private var openSegment = segmentManager.createOpenSegment()

    // assumes thread local environment
    override fun compress() {
        closedSegments.compact()
    }

    override fun put(key: K, value: V) {
        openSegment[key] = value
        if (openSegment.isFull()) {
            closedSegments.accept(openSegment)
            openSegment = segmentManager.createOpenSegment()
        }
    }

    override fun contains(key: K): Boolean = when(key in openSegment) {
        true -> true
        false -> closedSegments.any { key in it }
    }

    override fun get(key: K): V? {
        val openValue = openSegment.getWithTombstone(key)

        if (openValue != null) {
            logger.debug { "Found key $key in open segment" }
            return openValue
                .takeUnless { possiblyArrayEquals(it, tombstone) }
        }

        var segCounter = 0
        for (segment in closedSegments) {
            logger.trace { "Searching segment ${segCounter++} for key $key" }
            val value = segment.logKV.getWithTombstone(key)

            if (value != null) {
                logger.debug { "Found key $key in segment $segCounter" }
                return value
                    .takeUnless { possiblyArrayEquals(it, tombstone) }
            }
        }
        return null
    }

    override fun delete(key: K) {
        openSegment.delete(key)
    }

    override fun clear() {
        openSegment.clear()
        closedSegments.clear()
    }

}

class LSMKeyValueStoreFactory<K, V>(private val tombstone: V, private val dispatcher: CoroutineDispatcher) {

    fun createLSMKeyValueStore(segmentManager: SegmentManager<K, V>): LSMKeyValueStore<K, V> {

        return SegmentedLSM(
            segmentManager,
            tombstone,
        ).let { RecurrentMergeDecorator(it, createRecurrentMerge(it)) }
    }

    private fun createRecurrentMerge(lsm: LSMKeyValueStore<K, V>) = OpsBasedRecurrentJob(
        lsm::compress,
        10_000,
        dispatcher
    )

}


