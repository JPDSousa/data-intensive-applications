package org.example.index

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.ApplicationTest
import org.example.TestInstance
import org.example.generator.LongGenerator
import org.example.generator.StringGenerator
import org.example.test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInfo

internal abstract class CheckpointableIndexTest<K>: ApplicationTest(), IndexTest<K> {

    abstract override fun instances(): Sequence<TestInstance<CheckpointableIndex<K>>>

    abstract fun factories(): Sequence<TestInstance<CheckpointableIndexFactory<K>>>

    @TestFactory fun `checkpointable index should be recoverable`(info: TestInfo) = factories().test(info) { factory ->

        val expected = (1L..100L).map { IndexEntry(nextKey(), it) }

        val indexName = "2bRecovered-${nextKey()}"
        val index = factory.create(indexName)

        index.putAllOffsets(expected)
        index.checkpoint()

        // should recover the index from file
        val recoveredIndexed = factory.create(indexName)

        expected.forEach {
            assertEquals(it.offset, recoveredIndexed[it.key])
        }
    }

}

internal class CheckpointableStringIndexTest: CheckpointableIndexTest<String>() {

    private val valueIterator = valueGenerator.generate().iterator()

    @ExperimentalSerializationApi
    override fun instances(): Sequence<TestInstance<CheckpointableIndex<String>>> = indexes.generate()

    override fun nextKey() = when {
        valueIterator.hasNext() -> valueIterator.next()
        else -> throw NoSuchElementException("No more keys")
    }

    @ExperimentalSerializationApi
    override fun factories(): Sequence<TestInstance<CheckpointableIndexFactory<String>>> = factories.generate()

    companion object {

        @JvmStatic
        private val valueGenerator: StringGenerator = application.koin.get()

        @JvmStatic
        private val indexes = application.koin.get<StringCheckpointableIndexes>()

        @JvmStatic
        private val factories = application.koin.get<StringCheckpointableIndexFactories>()
    }

}

internal class CheckpointableLongTest: CheckpointableIndexTest<Long>() {

    private val valueIterator = valueGenerator.generate().iterator()

    @ExperimentalSerializationApi
    override fun instances(): Sequence<TestInstance<CheckpointableIndex<Long>>> = indexes
        .generate()

    override fun nextKey() = when {
        valueIterator.hasNext() -> valueIterator.next()
        else -> throw NoSuchElementException("No more keys")
    }

    @ExperimentalSerializationApi
    override fun factories(): Sequence<TestInstance<CheckpointableIndexFactory<Long>>> = factories
        .generate()

    companion object {

        @JvmStatic
        private val valueGenerator: LongGenerator = application.koin.get()

        @JvmStatic
        private val indexes = application.koin.get<LongCheckpointableIndexes>()

        @JvmStatic
        private val factories = application.koin.get<LongCheckpointableIndexFactories>()

    }
}
