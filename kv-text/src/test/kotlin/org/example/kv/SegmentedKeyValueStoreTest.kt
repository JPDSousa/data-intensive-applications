package org.example.kv

import org.example.TestInstance
import java.nio.file.Files

internal class SegmentedKeyValueStoreTest: KeyValueStoreTest {

    private val smallSegment = Files.createTempDirectory("small-kv-")
    private val longSegment = Files.createTempDirectory("long-kv-")

    override fun instances() = sequenceOf(
            TestInstance("Small Segments", SegmentedKeyValueStore(smallSegment, 5) as KeyValueStore),
            TestInstance("Long Segments", SegmentedKeyValueStore(longSegment) as KeyValueStore)
    )
}
