package org.example.index

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.serializer
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.Encoders
import org.example.log.LogFactories
import org.example.test
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestFactory
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

internal abstract class CheckpointableIndexTest<K>: IndexTest<K> {

    internal val uniqueGenerator = AtomicLong()

    abstract override fun instances(): Sequence<TestInstance<CheckpointableIndex<K>>>

    abstract fun factories(): Sequence<TestInstance<CheckpointableIndexFactory<K>>>

    @TestFactory fun `checkpointable index should be recoverable`() = factories().test { factory ->

        val expected = (1..100).map { IndexEntry(nextKey(), it.toLong()) }

        val indexName = "2bRecovered-${nextKey()}"
        val index = factory.create(indexName)

        index.putAllOffsets(expected)
        index.checkpoint()

        // should recover the index from file
        val recoveredIndexed = factory.create(indexName)

        expected.forEach { assertEquals(it.offset, recoveredIndexed.getOffset(it.key)) }
    }

    companion object {

        internal val resources = TestResources()

        @JvmStatic
        internal val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

        @JvmStatic
        internal val indexes = CheckpointableIndexes(resources, LogFactories(Encoders()), dispatcher)

        @JvmStatic
        @AfterAll
        fun closeResources() {
            resources.close()
        }

    }

}

internal class CheckpointableStringIndexTest: CheckpointableIndexTest<String>() {

    @ExperimentalSerializationApi
    override fun instances(): Sequence<TestInstance<CheckpointableIndex<String>>> = indexes.indexes(serializer())

    override fun nextKey() = uniqueGenerator.getAndIncrement().toString()

    @ExperimentalSerializationApi
    override fun factories(): Sequence<TestInstance<CheckpointableIndexFactory<String>>> = indexes
        .comparableIndexFactories(serializer())

}

internal class CheckpointableBinaryLongTest: CheckpointableIndexTest<Long>() {

    @ExperimentalSerializationApi
    override fun instances(): Sequence<TestInstance<CheckpointableIndex<Long>>> = indexes
        .nonComparableIndexes(serializer())

    override fun nextKey() = uniqueGenerator.getAndIncrement()

    @ExperimentalSerializationApi
    override fun factories(): Sequence<TestInstance<CheckpointableIndexFactory<Long>>> = indexes
        .nonComparableIndexFactories(serializer())

}
