package org.example.log

import org.example.TestInstance
import org.example.TestResources
import org.example.assertPossiblyArrayEquals
import org.example.test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.TestInfo

interface LogFactoryTest<T> {

    fun instances(): Sequence<TestInstance<LogFactory<T>>>

    val resources: TestResources

    fun nextValue(): T

    @TestFactory fun `create should load file content`(info: TestInfo) = instances().test(info) { logFactory ->
        val path = resources.allocateTempFile("log-", ".log")
        val expectedValues = (1..100).map { nextValue() }
        val log = logFactory.create(path)
        log.appendAll(expectedValues.asSequence())

        val recoveredLog = logFactory.create(path)
        val actualValues = recoveredLog.useEntries { it.toList() }

        expectedValues.zip(actualValues).forEach { (expected, actual) ->
            assertPossiblyArrayEquals(expected, actual)
        }
    }

}
