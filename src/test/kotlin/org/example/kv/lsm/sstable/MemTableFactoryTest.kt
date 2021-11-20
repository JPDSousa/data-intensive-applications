package org.example.kv.lsm.sstable

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.core.spec.style.shouldSpec
import io.kotest.matchers.maps.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.*
import io.kotest.property.checkAll
import org.example.TestInstance
import org.example.bootstrapApplication
import org.example.kv.Tombstone
import org.example.kv.lsm.SegmentDirectories
import org.example.kv.lsm.SegmentDirectory
import org.example.possiblyArrayEquals

fun <K: Comparable<K>, V> memTableFactoryTests(
    gen: Gen<TestInstance<MemTableFactory<K, V>>>,
    segmentDirectories: Gen<TestInstance<SegmentDirectory>>,
    keyGen: Arb<K>,
    valueGen: Arb<V>,
    config: PropTestConfig = PropTestConfig(maxFailure = 3, iterations = 100),
) = shouldSpec {

    should("be recoverable") {
        checkAll(config, gen, segmentDirectories) { memTableSpec, segmentDirectorySpec ->

            val segmentId = 123
            val expectedEntries = (0..100).associate { Pair(keyGen.next(), valueGen.next()) }

            val segmentDirectory = segmentDirectorySpec.instance()
            val memTableFactory = memTableSpec.instance()

            val memTable = memTableFactory.createMemTable(segmentDirectory, segmentId)
            memTable.putAll(expectedEntries)

            val recoveredMemTable = memTableFactory.createMemTable(segmentDirectory, segmentId)

            val actualEntries = recoveredMemTable.associate { (key, value) -> Pair(key, value) }

            // TODO aggregate this in custom matcher
            actualEntries shouldHaveSize expectedEntries.size
            for (expectedEntry in expectedEntries) {
                actualEntries[expectedEntry.key] shouldBe expectedEntry.value
                (expectedEntry.key in actualEntries) shouldBe true
            }
        }
    }
}

internal class StringStringMemTableFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val factories: StringStringMemTableFactories = application.koin.get()
    val segmentDirectories: SegmentDirectories = application.koin.get()

    include(memTableFactoryTests(
        factories.toArb(),
        segmentDirectories.toArb(),
        Arb.string(),
        Arb.string()
            // TODO fix the harcoded filter. This should be specific to the TestInstance we're using
            .filterNot { it == Tombstone.string },
    ))
})

internal class LongByteArrayMemTableFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val factories: LongByteArrayMemTableFactories = application.koin.get()
    val segmentDirectories: SegmentDirectories = application.koin.get()

    include(memTableFactoryTests(
        factories.toArb(),
        segmentDirectories.toArb(),
        Arb.long(),
        Arb.byteArray(Arb.int(0..100), Arb.byte())
            // TODO fix the harcoded filter. This should be specific to the TestInstance we're using
            .filterNot { possiblyArrayEquals(it, Tombstone.byte) },
    ))
})
