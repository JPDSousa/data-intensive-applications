package org.example.index

import io.kotest.common.DelicateKotest
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.core.spec.style.shouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.*
import io.kotest.property.checkAll
import org.example.TestInstance
import org.example.bootstrapApplication

internal class CheckpointableStringIndexSpec: ShouldSpec({

    val application = bootstrapApplication()
    val indexes: StringCheckpointableIndexes = application.koin.get()

    include(indexTests(
        indexes.toArb(),
        Arb.string()
    ))
})

internal class CheckpointableLongIndexSpec: ShouldSpec({

    val application = bootstrapApplication()
    val indexes: LongCheckpointableIndexes = application.koin.get()

    include(indexTests(
        indexes.toArb(),
        Arb.long()
    ))
})

@DelicateKotest
fun <K> checkpointableIndexFactoryTests(
    factories: Gen<TestInstance<CheckpointableIndexFactory<K>>>,
    keyGen: Arb<K>,
    config: PropTestConfig = PropTestConfig(maxFailure = 3, iterations = 100),
) = shouldSpec {
    
    should("be recoverable") {
        checkAll(config, factories) { spec ->
            val factory = spec.instance()
            val distinctKeyGen = keyGen.distinct()
            val expected = (1L..100L).map { IndexEntry(distinctKeyGen.next(), it) }

            val indexName = "2bRecovered-${keyGen.next()}"
            val index = factory.create(indexName)

            index.putAllOffsets(expected)
            index.checkpoint()

            // should recover the index from file
            val recoveredIndexed = factory.create(indexName)

            expected.forEach {
                recoveredIndexed[it.key] shouldBe it.offset
                index[it.key] shouldBe it.offset
            }
        }
    }
}

@DelicateKotest
internal class CheckpointableStringIndexFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val indexes: StringCheckpointableIndexFactories = application.koin.get()

    include(checkpointableIndexFactoryTests(
        indexes.toArb(),
        Arb.string(codepoints = Codepoint.alphanumeric())
    ))
})

@DelicateKotest
internal class CheckpointableLongIndexFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val indexes: LongCheckpointableIndexFactories = application.koin.get()

    include(checkpointableIndexFactoryTests(
        indexes.toArb(),
        Arb.long()
    ))
})
