package org.example.kv.lsm.sstable

import org.example.ApplicationTest
import org.example.TestInstance
import org.example.concepts.asImmutableDictionaryMixin
import org.example.generator.ByteArrayGenerator
import org.example.generator.LongGenerator
import org.example.generator.StringGenerator
import org.example.GetAssertion
import org.example.kv.lsm.SegmentDirectories
import org.example.kv.lsm.SegmentDirectory
import org.example.test
import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInfo

private data class FactoryCreation<K: Comparable<K>, V>(
    val memTableFactory: TestInstance<MemTableFactory<K, V>>,
    val segmentDirectory: TestInstance<SegmentDirectory>
)

interface MemTableFactoryTest<K: Comparable<K>, V> {

    fun nextKey(): K

    fun nextValue(): V

    fun instances(): Sequence<TestInstance<MemTableFactory<K, V>>>

    fun segmentDirectories(): Sequence<TestInstance<SegmentDirectory>>

    @TestFactory fun `memTable should be recoverable`(info: TestInfo) = instances().flatMap { memTableFactory ->
        segmentDirectories().map { segmentDirectory ->
            TestInstance("Tuple($memTableFactory, $segmentDirectory)") {
                FactoryCreation(memTableFactory, segmentDirectory)
            }
        }
    }.test(info) { factory ->

        val segmentId = 123
        val expectedEntries = (0..100).associate { Pair(nextKey(), nextValue()) }

        val segmentDirectory = factory.segmentDirectory.instance()
        val memTableFactory = factory.memTableFactory.instance()

        val memTable = memTableFactory.createMemTable(segmentDirectory, segmentId)
        memTable.putAll(expectedEntries)

        val recoveredMemTable = memTableFactory.createMemTable(segmentDirectory, segmentId)

        val actualEntries = recoveredMemTable.associate { (key, value) -> Pair(key, value) }

        assertEquals(expectedEntries.size, actualEntries.size)
        assertAll(expectedEntries.map { GetAssertion(actualEntries.asImmutableDictionaryMixin(), it.key, it.value) })
    }

}

internal abstract class AbstractMemTableFactoryTest<K: Comparable<K>, V>: ApplicationTest(), MemTableFactoryTest<K, V> {

    override fun segmentDirectories() = segmentDirectories.generate()

    companion object {

        @JvmStatic
        private val segmentDirectories: SegmentDirectories = application.koin.get()

    }
}

internal class StringStringMemTableFactoryTest: AbstractMemTableFactoryTest<String, String>() {

    private val keyGenerator = stringGenerator.generate().iterator()

    override fun nextKey() = when {
        keyGenerator.hasNext() -> keyGenerator.next()
        else -> throw NoSuchElementException("No more values!")
    }

    override fun nextValue() = nextKey()

    override fun instances() = factories.generate()

    companion object {

        @JvmStatic
        private val stringGenerator: StringGenerator = application.koin.get()

        @JvmStatic
        private val factories: StringStringMemTableFactories = application.koin.get()

    }
}

internal class LongByteArrayMemTableFactoryTest: AbstractMemTableFactoryTest<Long, ByteArray>() {

    private val keyGenerator = longGenerator.generate().iterator()
    private val valueGenerator = byteArrayGenerator.generate().iterator()

    override fun nextKey() = when {
        keyGenerator.hasNext() -> keyGenerator.next()
        else -> throw NoSuchElementException("No more values!")
    }

    override fun nextValue() = when {
        valueGenerator.hasNext() -> valueGenerator.next()
        else -> throw NoSuchElementException("No more values!")
    }

    override fun instances() = factories.generate()

    companion object {

        @JvmStatic
        private val longGenerator: LongGenerator = application.koin.get()

        @JvmStatic
        private val byteArrayGenerator: ByteArrayGenerator = application.koin.get()

        @JvmStatic
        private val factories: LongByteArrayMemTableFactories = application.koin.get()

    }

}
