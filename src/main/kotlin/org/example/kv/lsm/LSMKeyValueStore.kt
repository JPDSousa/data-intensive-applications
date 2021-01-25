package org.example.kv.lsm

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.example.kv.KeyValueStore
import org.example.kv.LogBasedKeyValueStore
import org.example.kv.LogBasedKeyValueStoreFactory
import org.example.kv.Tombstone
import org.example.log.LogFactory
import org.example.lsm.LSMStructure
import org.example.possiblyArrayEquals
import java.nio.file.Path
import java.util.concurrent.Executors

internal class LSMKeyValueStore<K, V>(private val segments: LSMStructure<LogBasedKeyValueStore<K, V>>,
                                      private val tombstone: V,
                                      private val compactCycle: Long = 1000,
                                      private val compactPooling: Long = 1000 * 60): KeyValueStore<K, V> {

    private val logger = KotlinLogging.logger {}

    @Volatile
    private var opsWithoutCompact: Long = 0

    init {
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
        GlobalScope.launch(dispatcher) {
            while (true) {
                logger.trace { "Waiting $compactPooling to check again for the need of compacting" }
                delay(compactPooling)
                val opsWithoutCompact = this@LSMKeyValueStore.opsWithoutCompact
                if (opsWithoutCompact >= compactCycle) {
                    logger.info { "Reached $opsWithoutCompact since last compact. Triggering compact operation" }
                    segments.compact()
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
        segments.openSegment().structure.put(key, value)
    }

    override fun get(key: K): V? {
        var segCounter = 0
        for (segment in segments) {
            logger.trace { "Searching segment ${segCounter++} for key $key" }
            val kvs = segment.structure
            val value = kvs.getWithTombstone(key)

            if (possiblyArrayEquals(value, tombstone)) {
                logger.trace { "Found tombstone. Returning null" }
                return null
            }

            if (value != null) {
                return value
            }
        }
        return null
    }

    override fun delete(key: K) {
        this.segments.openSegment().structure.delete(key)
    }

    override fun clear() = segments.clear()
}

class LSMKeyValueStoreFactory<E, K, V>(private val logFactory: LogFactory<E>,
                                       private val logKVSFactory: LogBasedKeyValueStoreFactory<E, K, V>,
                                       private val tombstone: V) {


    fun createLSMKeyValueStore(kvDir: Path): KeyValueStore<K, V> = LSMSegmentFactory(
            kvDir,
            logFactory,
            logKVSFactory,
            segmentSize
    ).let {
        LSMKeyValueStore(
                LSMStructure(it, segmentSize, KeyValueLogMergeStrategy(it)),
                tombstone
        )
    }

    companion object {

        private const val segmentSize: Long = 1024 * 1024

    }
}


