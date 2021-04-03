package org.example.kv

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import org.example.DataEntrySerializer
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.Encoders
import org.example.kv.lsm.KeyValueLogMergeStrategy
import org.example.kv.lsm.LSMKeyValueStoreFactory
import org.example.kv.lsm.LogBasedSegmentFactory
import org.example.kv.sstable.SSTableMergeStrategy
import org.example.kv.sstable.SSTableSegmentFactory
import org.example.log.LogEncoderFactory
import org.example.log.LogFactories
import org.example.lsm.SegmentDirectory
import org.example.size.SizeCalculator

class KeyValueStores(private val iKVs: LogKeyValueStores,
                     private val logFactories: LogFactories,
                     private val encoders: Encoders,
                     private val byteArrayCalculator: SizeCalculator<ByteArray>,
                     private val stringSizeCalculator: SizeCalculator<String>,
                     private val resources: TestResources) {

    @ExperimentalSerializationApi
    fun binaryKeyValueStores(): Sequence<TestInstance<KeyValueStore<ByteArray, ByteArray>>> = sequence {

        val serializer: KSerializer<Map.Entry<ByteArray, ByteArray>> = DataEntrySerializer(serializer(), serializer())

        for (binaryInstance in logFactories.binaryInstances()) {
            for (binaryKeyValueStore in iKVs.binaryKeyValueStores()) {
                for (encoder in encoders.binaries(serializer)) {

                    val logKVSFactory = binaryKeyValueStore.instance()
                    val lsmKvsFactory = LSMKeyValueStoreFactory(
                        logKVSFactory,
                        Tombstone.byte
                    )

                    yield(TestInstance("Binary Append-only LSM Key Value Store") {

                        val kvDir = resources.allocateTempDir("segmented-")
                        val pairLogFactory = LogEncoderFactory(binaryInstance.instance(), encoder.instance())
                        val segmentDirectory = SegmentDirectory(kvDir)
                        val segmentFactory = LogBasedSegmentFactory(
                            segmentDirectory,
                            pairLogFactory,
                            logKVSFactory
                        )
                        val mergeStrategy = KeyValueLogMergeStrategy(
                            segmentFactory,
                            byteArrayCalculator,
                            byteArrayCalculator
                        )

                        lsmKvsFactory.createLSMKeyValueStore(segmentFactory, mergeStrategy)
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
                    val lsmKvsFactory = LSMKeyValueStoreFactory(
                        logKVSFactory,
                        Tombstone.string
                    )

                    yield(TestInstance("String Append-only LSM Key Value Store") {

                        val kvDir = resources.allocateTempDir("segmented-")
                        val pairLogFactory = LogEncoderFactory(stringInstance.instance(), encoder.instance())
                        val segmentDirectory = SegmentDirectory(kvDir)
                        val segmentFactory = LogBasedSegmentFactory(
                            segmentDirectory,
                            pairLogFactory,
                            logKVSFactory
                        )
                        val mergeStrategy = KeyValueLogMergeStrategy(
                            segmentFactory,
                            stringSizeCalculator,
                            stringSizeCalculator
                        )

                        lsmKvsFactory.createLSMKeyValueStore(segmentFactory, mergeStrategy)
                    })

                    yield(TestInstance("Binary SSTable LSM Key Value Store") {

                        val kvDir = resources.allocateTempDir("segmented-")
                        val pairLogFactory = LogEncoderFactory(stringInstance.instance(), encoder.instance())
                        val segmentDirectory = SegmentDirectory(kvDir)
                        val segmentFactory = SSTableSegmentFactory(
                            segmentDirectory,
                            pairLogFactory,
                            logKVSFactory,
                            stringSizeCalculator,
                            stringSizeCalculator
                        )
                        val mergeStrategy = SSTableMergeStrategy(segmentFactory)

                        lsmKvsFactory.createLSMKeyValueStore(segmentFactory, mergeStrategy)
                    })

                }
            }
        }
    }

}
