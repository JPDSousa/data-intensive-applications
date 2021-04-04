package org.example.index

import org.example.TestInstance
import java.util.concurrent.atomic.AtomicLong

internal class TreeIndexTest: IndexTest<String> {

    private val uniqueGenerator = AtomicLong()

    override fun instances(): Sequence<TestInstance<Index<String>>> = indexes.treeIndexes()

    override fun nextKey() = uniqueGenerator.getAndIncrement().toString()

    companion object {

        @JvmStatic
        internal val indexes = Indexes()
    }
}
