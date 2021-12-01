package org.example.kv.lsm.sstable

import io.kotest.common.DelicateKotest
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.core.spec.style.shouldSpec
import io.kotest.matchers.maps.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.*
import io.kotest.property.checkAll
import kotlinx.serialization.ExperimentalSerializationApi
import org.example.bootstrapApplication
import org.example.defaultPropTestConfig
import org.example.kv.Tombstone
import org.example.kv.lsm.SegmentDirectories
import org.example.kv.lsm.SegmentDirectory
import org.example.possiblyArrayEquals

fun <K: Comparable<K>, V> memTableFactoryTests(
    gen: Gen<MemTableFactory<K, V>>,
    segmentDirectories: Gen<SegmentDirectory>,
    keyGen: Arb<K>,
    valueGen: Arb<V>,
    config: PropTestConfig = defaultPropTestConfig,
) = shouldSpec {

    should("be recoverable") {
        checkAll(config, gen, segmentDirectories) { memTableFactory, segmentDirectory ->

            val segmentId = 123
            val expectedEntries = (0..100).associate { Pair(keyGen.next(), valueGen.next()) }

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

@DelicateKotest
@ExperimentalSerializationApi
internal class StringStringMemTableFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val factories: StringStringMemTableFactories = application.koin.get()
    val segmentDirectories: SegmentDirectories = application.koin.get()

    include(memTableFactoryTests(
        factories.gen,
        segmentDirectories.gen,
        Arb.string(),
        Arb.string()
            // TODO fix the harcoded filter. This should be specific to the TestInstance we're using
            .filterNot { it == Tombstone.string },
    ))
})

@DelicateKotest
@ExperimentalSerializationApi
internal class LongByteArrayMemTableFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val factories: LongByteArrayMemTableFactories = application.koin.get()
    val segmentDirectories: SegmentDirectories = application.koin.get()

    include(memTableFactoryTests(
        factories.gen,
        segmentDirectories.gen,
        Arb.long(),
        Arb.byteArray(Arb.int(0..100), Arb.byte())
            // TODO fix the harcoded filter. This should be specific to the TestInstance we're using
            .filterNot { possiblyArrayEquals(it, Tombstone.byte) },
    ))
})
