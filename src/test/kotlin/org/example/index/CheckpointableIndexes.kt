package org.example.index

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.JsonStringEncoder
import org.example.encoder.ProtobufBinaryEncoder
import org.example.log.LogFactories
import java.util.concurrent.atomic.AtomicLong

class CheckpointableIndexes(private val resources: TestResources,
                            private val logs: LogFactories,
                            private val dispatcher: CoroutineDispatcher) {

    private val generator = AtomicLong()

    @ExperimentalSerializationApi
    fun <K : Comparable<K>> indexes(serializer: KSerializer<IndexEntry<K>>)
            : Sequence<TestInstance<CheckpointableIndex<K>>>
            = comparableIndexes(serializer) + nonComparableIndexes(serializer)

    @ExperimentalSerializationApi
    fun <K: Comparable<K>> comparableIndexes(serializer: KSerializer<IndexEntry<K>>):
            Sequence<TestInstance<CheckpointableIndex<K>>> = indexes(TreeIndexFactory(), serializer)

    @ExperimentalSerializationApi
    fun <K> nonComparableIndexes(serializer: KSerializer<IndexEntry<K>>)
            : Sequence<TestInstance<CheckpointableIndex<K>>> = indexes(HashIndexFactory(), serializer)

    @ExperimentalSerializationApi
    private fun <K> indexes(innerIndexFactory: IndexFactory<K>, serializer: KSerializer<IndexEntry<K>>):
            Sequence<TestInstance<CheckpointableIndex<K>>> = indexFactories(innerIndexFactory, serializer)
        .map { case ->
            TestInstance(case.name) {
                case.instance().create("CheckpointableIndex${generator.getAndIncrement()}")
            }
        }

    @ExperimentalSerializationApi
    fun <K> nonComparableIndexFactories(serializer: KSerializer<IndexEntry<K>>) = indexFactories(
        HashIndexFactory(),
        serializer
    )

    @ExperimentalSerializationApi
    fun <K: Comparable<K>> comparableIndexFactories(serializer: KSerializer<IndexEntry<K>>) = indexFactories(
        TreeIndexFactory(),
        serializer
    )

    @ExperimentalSerializationApi
    private fun <K> indexFactories(innerIndexFactory: IndexFactory<K>, serializer: KSerializer<IndexEntry<K>>)
    : Sequence<TestInstance<CheckpointableIndexFactory<K>>> = sequence {

        for (lineLogInstance in logs.lineLogInstances()) {

            yield(TestInstance("Checkpointable String Index ~ ${lineLogInstance.name}") {
                val indexDir = resources.allocateTempDir("index-string-")
                CheckpointableIndexFactory(
                    innerIndexFactory,
                    indexDir,
                    IndexEntryLogFactory(lineLogInstance.instance(), JsonStringEncoder(serializer)),
                    10_000,
                    dispatcher
                )
            })
        }

        for (binaryInstance in logs.binaryInstances()) {

            yield(TestInstance("Checkpointable Binary Index ~ ${binaryInstance.name}") {
                val indexDir = resources.allocateTempDir("index-binary-")
                CheckpointableIndexFactory(
                    innerIndexFactory,
                    indexDir,
                    IndexEntryLogFactory(binaryInstance.instance(), ProtobufBinaryEncoder(serializer)),
                    10_000,
                    dispatcher
                )
            })
        }
    }
}
