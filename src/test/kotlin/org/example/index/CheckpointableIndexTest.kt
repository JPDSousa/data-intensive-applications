package org.example.index

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.serializer
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.Encoders
import org.example.log.LogFactories
import org.junit.jupiter.api.AfterAll
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

internal abstract class AbstractLogCheckpointableIndexTest<K>: IndexTest<K> {

    internal val uniqueGenerator = AtomicLong()

    companion object {

        @JvmStatic
        internal val resources = TestResources()

        @JvmStatic
        internal val dispatcher= Executors.newSingleThreadExecutor().asCoroutineDispatcher()

        @JvmStatic
        internal val indexes = Indexes(LogFactories(Encoders()), resources, dispatcher)

        @JvmStatic
        @AfterAll
        fun closeResources() {
            resources.close()
        }

    }

}

internal class CheckpointableStringIndexTest: AbstractLogCheckpointableIndexTest<String>() {

    @ExperimentalSerializationApi
    override fun instances(): Sequence<TestInstance<Index<String>>> = indexes.instances(serializer())

    override fun nextKey() = uniqueGenerator.getAndIncrement().toString()

}

internal class CheckpointableBinaryIndexTest: AbstractLogCheckpointableIndexTest<ByteArray>() {

    @ExperimentalSerializationApi
    override fun instances(): Sequence<TestInstance<Index<ByteArray>>> = indexes.nonComparableInstances(serializer())

    override fun nextKey() = uniqueGenerator.getAndIncrement()
            .toString()
            .toByteArray()

}
