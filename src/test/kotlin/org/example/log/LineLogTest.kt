package org.example.log

import io.kotest.common.DelicateKotest
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.next
import io.kotest.property.arbitrary.string
import io.kotest.property.checkAll
import kotlinx.serialization.ExperimentalSerializationApi
import org.example.bootstrapApplication
import org.example.defaultPropTestConfig

@DelicateKotest
@ExperimentalSerializationApi
internal class LineLogSpec: ShouldSpec({

    val application = bootstrapApplication()
    val gen = application.koin.get<StringLogs>(lineLogQ).gen
    val valueGen = Arb.string()
    val config = defaultPropTestConfig

    include(logTests(
        gen,
        valueGen,
        config,
    ))

    fun entriesShouldBePartitionedByLines(log: Log<String>, append: (Sequence<String>) -> Unit) {

        val entries = (1..100).map { valueGen.next() }
            .asSequence()
        val expected = entries.joinToString("\n")

        val initialSize = log.useEntries { it.count() }

        append(entries)

        val testContent = log.useEntries { it.drop(initialSize).joinToString("\n") }

        testContent shouldBe expected
    }

    should("entries should be partitioned by lines") {
        checkAll(config, gen) { log ->
            entriesShouldBePartitionedByLines(log) { entries ->
                entries.forEach { log.append(it) }
            }
        }
    }

    should("entries should be partitioned by lines (appendAll)") {
        checkAll(config, gen) { log ->
            entriesShouldBePartitionedByLines(log) { entries ->
                log.appendAll(entries)
            }
        }
    }

})

@DelicateKotest
@ExperimentalSerializationApi
internal class LineLogFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val logFactories: StringLogFactories = application.koin.get(lineLogQ)

    include(logFactoryTests(
        logFactories.gen,
        Arb.string(),
    ))
})
