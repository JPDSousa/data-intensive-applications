package org.example.kv.lsm

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.example.kv.KeyValueStore
import org.example.possiblyArrayEquals
import java.util.concurrent.Executors

interface LSMKeyValueStore<K, V>: KeyValueStore<K, V> {

    fun compact()
}

private class OpsCycleDecorator<K, V>(private val decorated: LSMKeyValueStore<K, V>,
                                      private val compactCycle: Long = 1000,
                                      private val compactPooling: Long = 1000 * 60): LSMKeyValueStore<K, V> {

    private val logger = KotlinLogging.logger {}

    @Volatile
    private var opsWithoutCompact: Long = 0

    init {
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
        GlobalScope.launch(dispatcher) {
            while (true) {
                logger.trace { "Waiting $compactPooling to check again for the need of compacting" }
                delay(compactPooling)
                val opsWithoutCompact = this@OpsCycleDecorator.opsWithoutCompact
                if (opsWithoutCompact >= compactCycle) {
                    logger.info { "Reached $opsWithoutCompact since last compact. Triggering compact operation" }
                    compact()
                } else {
                    logger.debug { "$opsWithoutCompact ops since last compact. ${compactCycle - opsWithoutCompact} " +
                            "missing to trigger new compact operation"}
                }
            }
        }
    }

    override fun put(key: K, value: V) {
        decorated.put(key, value)
        opsWithoutCompact++
    }

    override fun get(key: K): V? = decorated.get(key)
        .also { opsWithoutCompact++ }

    override fun delete(key: K) {
        decorated.delete(key)
        opsWithoutCompact++
    }

    override fun clear() {
        decorated.clear()
        opsWithoutCompact = 0
    }

    override fun compact() {
        val opsWithoutCompact = this.opsWithoutCompact
        decorated.compact()
        this.opsWithoutCompact -= opsWithoutCompact
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

class LSMKeyValueStoreFactory<K, V>(private val tombstone: V) {

    fun createLSMKeyValueStore(segmentManager: SegmentManager<K, V>): LSMKeyValueStore<K, V> {

        return SegmentedLSM(
            segmentManager,
            tombstone,
        ).let { OpsCycleDecorator(it) }
    }

}


