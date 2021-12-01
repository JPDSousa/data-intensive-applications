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
import kotlinx.serialization.ExperimentalSerializationApi
import org.example.bootstrapApplication
import org.example.defaultPropTestConfig

@DelicateKotest
@ExperimentalSerializationApi
internal class CheckpointableStringIndexSpec: ShouldSpec({

    val application = bootstrapApplication()
    val indexes = application.koin.get<StringCheckpointableIndexes>().gen

    include(indexTests(
        indexes,
        Arb.string()
    ))
})

@DelicateKotest
@ExperimentalSerializationApi
internal class CheckpointableLongIndexSpec: ShouldSpec({

    val application = bootstrapApplication()
    val indexes = application.koin.get<LongCheckpointableIndexes>().gen

    include(indexTests(
        indexes,
        Arb.long()
    ))
})

@DelicateKotest
fun <K> checkpointableIndexFactoryTests(
    factories: Gen<CheckpointableIndexFactory<K>>,
    keyGen: Arb<K>,
    config: PropTestConfig = defaultPropTestConfig,
) = shouldSpec {
    
    should("be recoverable") {
        checkAll(config, factories) { factory ->
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

@ExperimentalSerializationApi
@DelicateKotest
internal class CheckpointableStringIndexFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val indexes = application.koin.get<StringCheckpointableIndexFactories>().gen

    include(checkpointableIndexFactoryTests(
        indexes,
        Arb.string(codepoints = Codepoint.alphanumeric())
    ))
})

@ExperimentalSerializationApi
@DelicateKotest
internal class CheckpointableLongIndexFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val indexes = application.koin.get<LongCheckpointableIndexFactories>().gen

    include(checkpointableIndexFactoryTests(
        indexes,
        Arb.long()
    ))
})
