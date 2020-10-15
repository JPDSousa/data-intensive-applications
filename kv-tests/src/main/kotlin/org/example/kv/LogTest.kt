package org.example.kv

import org.apache.commons.math3.distribution.ParetoDistribution
import org.example.TestInstance
import org.example.log.Log
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import kotlin.streams.asStream

interface LogTest {

    fun instances(): Sequence<TestInstance<Log>>

    @TestFactory fun `append on empty file should return 0`() = instances().map {
        dynamicTest(it.name) {
            assertEquals(0, it.instance.append("oneline"))
        }
    }.asStream()

    @TestFactory fun `append should sum offset`() = instances().map {
        dynamicTest(it.name) {
            val log = it.instance
            val firstOffset = log.append("oneline")
            val secondOffset = log.append("twoline")

            assertTrue(firstOffset < secondOffset)
        }
    }.asStream()

    @TestFactory fun `append should be readable`() = instances().map {
        dynamicTest(it.name) {
            val log = it.instance
            val expected = "oneline"
            log.append(expected)

            log.useLines {
                assertTrue(it.contains(expected))
            }
        }
    }.asStream()

    @TestFactory fun `entries should be partitioned by lines`() = instances().map {
        dynamicTest(it.name) {

            val distribution = ParetoDistribution()
            val log = it.instance
            val entries = (1..100).map { distribution.sample() }.map { "entryentryentry$it" }
            val expected = entries.joinToString("\n")

            entries.forEach { log.append(it) }

            val content = log.useLines { it.joinToString("\n") }

            assertEquals(expected, content)
        }
    }.asStream()

}
