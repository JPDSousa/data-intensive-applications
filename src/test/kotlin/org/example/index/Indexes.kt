package org.example.index

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.JsonStringEncoder
import org.example.encoder.ProtobufBinaryEncoder
import org.example.log.LogFactories
import java.util.concurrent.atomic.AtomicLong

class Indexes(private val logs: LogFactories, private val resources: TestResources) {

    private val generator = AtomicLong()

    @ExperimentalSerializationApi
    fun <K : Comparable<K>> instances(serializer: KSerializer<IndexEntry<K>>): Sequence<TestInstance<Index<K>>>
            = comparableInstances(serializer) + nonComparableInstances(serializer)

    @ExperimentalSerializationApi
    fun <K> nonComparableInstances(serializer: KSerializer<IndexEntry<K>>): Sequence<TestInstance<Index<K>>> {

        val factory = HashIndexFactory<K>()

        val volatileHash = TestInstance("Hash Index") {
            factory.create("HashIndex${generator.getAndIncrement()}")
        }

        return sequenceOf(volatileHash) + checkpointableNonComparableIndexes(serializer)
    }

    @ExperimentalSerializationApi
    private fun <K : Comparable<K>> comparableInstances(serializer: KSerializer<IndexEntry<K>>):
            Sequence<TestInstance<Index<K>>> {

        val factory = TreeIndexFactory<K>()
        val volatileTree = TestInstance("Tree Index") {
            factory.create("TreeIndex${generator.getAndIncrement()}")
        }

        return sequenceOf(volatileTree) + checkpointableComparableIndexes(serializer)
    }

    @ExperimentalSerializationApi
    private fun <K: Comparable<K>> checkpointableComparableIndexes(serializer: KSerializer<IndexEntry<K>>):
            Sequence<TestInstance<Index<K>>> = checkpointableIndexes(TreeIndexFactory(), serializer)

    @ExperimentalSerializationApi
    fun <K> checkpointableNonComparableIndexes(serializer: KSerializer<IndexEntry<K>>): Sequence<TestInstance<Index<K>>>
            = checkpointableIndexes(HashIndexFactory(), serializer)

    @ExperimentalSerializationApi
    private fun <K> checkpointableIndexes(innerIndexFactory: IndexFactory<K>, serializer: KSerializer<IndexEntry<K>>):
            Sequence<TestInstance<Index<K>>> =
            sequence {

                val indexDir = resources.allocateTempDir("index-")

                for (lineLogInstance in logs.lineLogInstances()) {
                    val stringFactory = CheckpointableIndexFactory(
                            innerIndexFactory,
                            indexDir,
                            IndexEntryLogFactory(lineLogInstance.instance(), JsonStringEncoder(serializer))
                    )

                    yield(TestInstance("Checkpointable String Index") {
                        stringFactory.create("CheckpointableIndex${generator.getAndIncrement()} ~ ${lineLogInstance
                                .name}")
                    })
                }

                for (binaryInstance in logs.binaryInstances()) {
                    val binaryFactory = CheckpointableIndexFactory(
                            innerIndexFactory,
                            indexDir,
                            IndexEntryLogFactory(binaryInstance.instance(), ProtobufBinaryEncoder(serializer))
                    )

                    yield(TestInstance("Checkpointable Binary Index") {
                        binaryFactory.create("CheckpointableIndex${generator.getAndIncrement()} ~ ${binaryInstance
                                .name}")
                    })
                }
            }

}
