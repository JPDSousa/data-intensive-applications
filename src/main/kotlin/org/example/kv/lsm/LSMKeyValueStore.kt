package org.example.kv.lsm

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.example.kv.KeyValueStore
import org.example.kv.LogBasedKeyValueStoreFactory
import org.example.lsm.*
import org.example.possiblyArrayEquals
import java.util.concurrent.Executors

internal class LSMKeyValueStore<K, V>(private val closedSegments: ClosedSegments<K, V>,
                                      private val segmentFactory: SegmentFactory<K, V>,
                                      private val tombstone: V,
                                      private val kvFactory: LogBasedKeyValueStoreFactory<K, V>,
                                      private val compactCycle: Long = 1000,
                                      private val compactPooling: Long = 1000 * 60): KeyValueStore<K, V> {

    private val logger = KotlinLogging.logger {}

    @Volatile
    private var opsWithoutCompact: Long = 0

    private var openSegment = segmentFactory.createOpenSegment()

    init {
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
        GlobalScope.launch(dispatcher) {
            while (true) {
                logger.trace { "Waiting $compactPooling to check again for the need of compacting" }
                delay(compactPooling)
                val opsWithoutCompact = this@LSMKeyValueStore.opsWithoutCompact
                if (opsWithoutCompact >= compactCycle) {
                    logger.info { "Reached $opsWithoutCompact since last compact. Triggering compact operation" }
                    closedSegments.compact()
                    // this is possibly not thread safe
                    this@LSMKeyValueStore.opsWithoutCompact -= opsWithoutCompact
                } else {
                    logger.debug { "$opsWithoutCompact ops since last compact. ${compactCycle - opsWithoutCompact} " +
                            "missing to trigger new compact operation"}
                }
            }
        }
    }

    override fun put(key: K, value: V) {
        openSegment.put(key, value)
        if (openSegment.isFull()) {
            closedSegments.accept(openSegment)
            openSegment = segmentFactory.createOpenSegment()
        }
    }

    override fun get(key: K): V? = getWithTombstone(key)
        .takeUnless { possiblyArrayEquals(it, tombstone) }

    override fun delete(key: K) {
        openSegment.delete(key)
    }

    override fun clear() {
        openSegment.clear()
        closedSegments.clear()
    }

    override fun getWithTombstone(key: K): V? {
        val openValue = openSegment.getWithTombstone(key)

        if (openValue != null) {
            logger.debug { "Found key $key in open segment" }
            return openValue
        }

        var segCounter = 0
        for (segment in closedSegments) {
            logger.trace { "Searching segment ${segCounter++} for key $key" }
            val kvSegment = kvFactory.createFromPair(segment.log)
            val value = kvSegment.getWithTombstone(key)

            if (value != null) {
                logger.debug { "Found key $key in segment $segCounter" }
                return value
            }
        }
        return null
    }
}

class LSMKeyValueStoreFactory<K, V>(
    private val logKVSFactory: LogBasedKeyValueStoreFactory<K, V>,
    private val tombstone: V
) {

    fun createLSMKeyValueStore(segmentFactory: SegmentFactory<K, V>,
                               mergeStrategy: SegmentMergeStrategy<K, V>): KeyValueStore<K, V> {

        return LSMKeyValueStore(
            ClosedSegments(mergeStrategy),
            segmentFactory,
            tombstone,
            logKVSFactory,
        )
    }

}


