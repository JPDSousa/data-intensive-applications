package org.example.log

import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertTrue
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

        val values = (1..300).map { "${it % 100}" }.shuffled(random)
        val remaining = values.distinct().toMutableList()

        values.forEach { segments!!.openSegment().append(it) }
        segments!!.compact { it }
        segments!!.from(0)
                .flatMap { it.lines(0) }
                .forEach { remaining.remove(it) }

        assertTrue(remaining.isEmpty())
    }

    companion object {

        private val random = Random(123)

    }
}
