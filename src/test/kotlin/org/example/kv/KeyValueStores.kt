package org.example.kv

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import org.example.DataEntrySerializer
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.Encoders
import org.example.kv.lsm.*
import org.example.kv.lsm.sstable.SSTableMergeStrategy
import org.example.kv.lsm.sstable.SSTableSegmentManager
import org.example.log.LogEncoderFactory
import org.example.log.LogFactories
import org.example.size.SizeCalculator

class KeyValueStores(private val iKVs: LogKeyValueStores,
                     private val logFactories: LogFactories,
                     private val encoders: Encoders,
                     private val byteArrayCalculator: SizeCalculator<ByteArray>,
                     private val stringSizeCalculator: SizeCalculator<String>,
                     private val resources: TestResources,
                     private val dispatcher: CoroutineDispatcher) {

    @ExperimentalSerializationApi
    fun binaryKeyValueStores(): Sequence<TestInstance<KeyValueStore<ByteArray, ByteArray>>> = sequence {

        val serializer: KSerializer<Map.Entry<ByteArray, ByteArray>> = DataEntrySerializer(serializer(), serializer())

        for (binaryInstance in logFactories.binaryInstances()) {
            for (binaryKeyValueStore in iKVs.binaryKeyValueStores()) {
                for (encoder in encoders.binaries(serializer)) {

                    val logKVSFactory = binaryKeyValueStore.instance()
                    val lsmKvsFactory = LSMKeyValueStoreFactory<ByteArray, ByteArray>(Tombstone.byte, dispatcher)

                    yield(TestInstance("Binary Append-only LSM Key Value Store") {

                        val kvDir = resources.allocateTempDir("segmented-")
                        val pairLogFactory = LogEncoderFactory(binaryInstance.instance(), encoder.instance())
                        val segmentDirectory = SegmentDirectory(kvDir)
                        val segmentFactory = SegmentFactory(
                            segmentDirectory,
                            pairLogFactory,
                            logKVSFactory,
                            segmentThreshold
                        )
                        val mergeStrategy = SequentialLogMergeStrategy(
                            segmentFactory,
                            segmentThreshold,
                            byteArrayCalculator,
                            byteArrayCalculator
                        )
                        val segmentManager = SequentialSegmentManager(
                            segmentDirectory,
                            pairLogFactory,
                            logKVSFactory,
                            segmentThreshold,
                            mergeStrategy
                        )

                        lsmKvsFactory.createLSMKeyValueStore(segmentManager)
                    })

                }
            }
        }
    }

    @ExperimentalSerializationApi
    fun stringKeyValueStores(): Sequence<TestInstance<KeyValueStore<String, String>>> = sequence {

        val serializer = DataEntrySerializer(serializer<String>(), serializer<String>())

        for (stringInstance in logFactories.stringInstances()) {
            for (stringKeyValueStore in iKVs.stringKeyValueStores()) {
                for (encoder in encoders.strings(serializer)) {

                    val logKVSFactory = stringKeyValueStore.instance()
                    val lsmKvsFactory = LSMKeyValueStoreFactory<String, String>(Tombstone.string, dispatcher)

                    yield(TestInstance("String Append-only LSM Key Value Store") {

                        val kvDir = resources.allocateTempDir("segmented-")
                        val pairLogFactory = LogEncoderFactory(stringInstance.instance(), encoder.instance())
                        val segmentDirectory = SegmentDirectory(kvDir)
                        val segmentFactory = SegmentFactory(
                            segmentDirectory,
                            pairLogFactory,
                            logKVSFactory,
                            segmentThreshold
                        )
                        val mergeStrategy = SequentialLogMergeStrategy(
                            segmentFactory,
                            segmentThreshold,
                            stringSizeCalculator,
                            stringSizeCalculator
                        )
                        val segmentManager = SequentialSegmentManager(
                            segmentDirectory,
                            pairLogFactory,
                            logKVSFactory,
                            segmentThreshold,
                            mergeStrategy
                        )

                        lsmKvsFactory.createLSMKeyValueStore(segmentManager)
                    })

                    yield(TestInstance("Binary SSTable LSM Key Value Store") {

                        val kvDir = resources.allocateTempDir("segmented-")
                        val pairLogFactory = LogEncoderFactory(stringInstance.instance(), encoder.instance())
                        val segmentDirectory = SegmentDirectory(kvDir)
                        val segmentFactory = SegmentFactory(
                            segmentDirectory,
                            pairLogFactory,
                            logKVSFactory,
                            segmentThreshold
                        )
                        val mergeStrategy = SSTableMergeStrategy(segmentFactory)
                        val segmentManager = SSTableSegmentManager(
                            segmentDirectory,
                            pairLogFactory,
                            logKVSFactory,
                            mergeStrategy,
                            segmentThreshold,
                            stringSizeCalculator,
                            stringSizeCalculator,
                            Tombstone.string
                        )

                        lsmKvsFactory.createLSMKeyValueStore(segmentManager)
                    })

                }
            }
        }
    }

    companion object {

        const val segmentThreshold: Long = 1024 * 1024
    }

}
