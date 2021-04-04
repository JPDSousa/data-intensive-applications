package org.example.index

import org.example.TestInstance
import java.util.concurrent.atomic.AtomicLong

class Indexes {

    private val generator = AtomicLong()

    fun <K> hashIndexes(): Sequence<TestInstance<Index<K>>> {

        val factory = HashIndexFactory<K>()

        val volatileHash = TestInstance("Hash Index") {
            factory.create("HashIndex${generator.getAndIncrement()}")
        }

        return sequenceOf(volatileHash)
    }

    fun <K: Comparable<K>> treeIndexes(): Sequence<TestInstance<Index<K>>> {

        val factory = TreeIndexFactory<K>()
        val transientTree = TestInstance("Tree Index") {
            factory.create("TreeIndex${generator.getAndIncrement()}")
        }

        return sequenceOf(transientTree)
    }

}
