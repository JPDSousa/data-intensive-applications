package org.example.log

import org.apache.commons.math3.distribution.ParetoDistribution
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.random.Random

internal class SegmentsTest {

    private var segments: Segments? = null

    @BeforeEach fun createObject(@TempDir path: Path) {

        segments = Segments(path, 50)
    }

    @Test
    fun `compact should maintain consistency`() {

        val distribution = ParetoDistribution()
        val values = (1..300).map { distribution.sample() }.map { "$it" }.shuffled(random)
        val remaining = values.distinct().toMutableList()

        values.forEach { segments!!.openSegment().append(it) }

        segments!!.compact { it }

        segments!!.from(0).forEach { log ->
            log.useLines { sequence ->
                sequence.forEach {
                    remaining.remove(it)
                }
            }
        }

        assertTrue(remaining.isEmpty())
    }

    companion object {

        private val random = Random(123)

    }
}
