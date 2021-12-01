package org.example.log

import io.kotest.core.spec.style.shouldSpec
import io.kotest.engine.spec.tempfile
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.next
import io.kotest.property.checkAll
import org.example.defaultPropTestConfig

fun <T> logFactoryTests(
    gen: Gen<LogFactory<T>>,
    valueGen: Arb<T>,
    config: PropTestConfig = defaultPropTestConfig,
) = shouldSpec {

    should("create should load file content") {
        checkAll(config, gen) { logFactory ->
            val path = tempfile().toPath()
            val expectedValues = (1..100).map { valueGen.next() }
            val log = logFactory.create(path)
            log.appendAll(expectedValues.asSequence())

            val recoveredLog = logFactory.create(path)
            val actualValues = recoveredLog.useEntries { it.toList() }

            expectedValues.zip(actualValues).forEach { (expected, actual) ->
                actual shouldBe expected
            }
        }
    }
}
