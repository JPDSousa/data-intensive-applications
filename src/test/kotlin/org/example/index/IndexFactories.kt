package org.example.index

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.Encoder
import org.example.encoder.JsonStringEncoder
import org.example.encoder.ProtobufBinaryEncoder
import org.example.log.BinaryLogFactory
import org.example.log.LineLogFactory
import org.example.log.LogFactory

class IndexFactories(private val resources: TestResources,
                     private val dispatcher: CoroutineDispatcher) {

    @ExperimentalSerializationApi
    fun <K> instances(serializer: KSerializer<IndexEntry<K>>): Sequence<TestInstance<IndexFactory<K>>> = sequence {

        yield(TestInstance("HashIndexFactory") {
            HashIndexFactory()
        })

        // TODO add other log factories
        yield(TestInstance("Checkpointable index with binary log") {
            checkpointableIndexFactory(
                    HashIndexFactory(),
                    BinaryLogFactory(),
                    ProtobufBinaryEncoder(serializer)
            )
        })

        yield(TestInstance("Checkpointable index with string log") {
            checkpointableIndexFactory(
                    HashIndexFactory(),
                    LineLogFactory(),
                    JsonStringEncoder(serializer))
        })
    }

    @ExperimentalSerializationApi
    fun <K: Comparable<K>> comparableInstances(
            serializer: KSerializer<IndexEntry<K>>): Sequence<TestInstance<IndexFactory<K>>> = sequence {

        yieldAll(instances(serializer))

        yield(TestInstance("Tree index factory") {
            TreeIndexFactory<K>()
        })
    }

    private fun <K, E> checkpointableIndexFactory(indexFactory: IndexFactory<K>,
                                                  logFactory: LogFactory<E>,
                                                  encoder: Encoder<IndexEntry<K>, E>) = CheckpointableIndexFactory(
        indexFactory,
        resources.allocateTempDir("index-"),
        IndexEntryLogFactory(
            logFactory,
            encoder
        ),
        10_000,
        dispatcher
    )

}
