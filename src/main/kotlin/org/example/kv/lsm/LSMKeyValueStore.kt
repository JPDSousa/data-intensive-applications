package org.example.kv.lsm

import kotlinx.coroutines.CoroutineDispatcher
import mu.KotlinLogging
import org.example.kv.KeyValueStore
import org.example.possiblyArrayEquals
import org.example.recurrent.OpsBasedRecurrentJob
import org.example.recurrent.RecurrentJob

interface LSMKeyValueStore<K, V>: KeyValueStore<K, V> {

    fun compact()
}

private class RecurrentMergeDecorator<K, V>(private val decorated: LSMKeyValueStore<K, V>,
                                            private val recurrentJob: RecurrentJob): LSMKeyValueStore<K, V> {

    override fun put(key: K, value: V) {
        decorated.put(key, value)
        recurrentJob.registerOperation()
    }

    override fun get(key: K): V? = decorated.get(key)
        .also { recurrentJob.registerOperation() }

    override fun delete(key: K) {
        decorated.delete(key)
        recurrentJob.registerOperation()
    }

    override fun clear() {
        decorated.clear()
        recurrentJob.reset()
    }

    override fun compact() {
        decorated.compact()
    }

}

private class SegmentedLSM<K, V>(private val segmentManager: SegmentManager<K, V>,
                                 private val tombstone: V): LSMKeyValueStore<K, V> {

    private val logger = KotlinLogging.logger {}

    private val closedSegments = segmentManager.loadClosedSegments()
    private var openSegment = segmentManager.createOpenSegment()

    // assumes thread local environment
    override fun compact() {
        closedSegments.compact()
    }

    override fun put(key: K, value: V) {
        openSegment.put(key, value)
        if (openSegment.isFull()) {
            closedSegments.accept(openSegment)
            openSegment = segmentManager.createOpenSegment()
        }
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
        lsm::compact,
        10_000,
        dispatcher
    )

}


