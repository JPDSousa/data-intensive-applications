package org.example.kv

import org.example.log.Log
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

interface LogTest {

    @Test fun `append on empty file should return 0`(log: Log) {

        assertEquals(0, log.append("oneline"))
    }

    @Test fun `append should sum offset`(log: Log) {

        val firstOffset = log.append("oneline")
        val secondOffset = log.append("twoline")

        assertTrue(firstOffset < secondOffset)
    }

    @Test fun `append should be readable`(log: Log) {

        val expected = "oneline"
        log.append(expected)

        assertTrue(log.lines().contains(expected))
    }

    @Test fun `entries should be partitioned by lines`(log: Log) {

        val entries = (1..100).map { "entryentryentry$it" }
        val expected = entries.joinToString("\n")

        entries.forEach { log.append(it) }

        val content = log.lines().joinToString("\n")

        assertEquals(expected, content)
    }

}
