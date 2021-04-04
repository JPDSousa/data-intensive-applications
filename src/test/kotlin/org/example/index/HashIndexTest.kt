package org.example.index

import org.example.TestInstance
import java.util.concurrent.atomic.AtomicLong

internal abstract class HashIndexTest<K>: IndexTest<K> {

    internal val uniqueGenerator = AtomicLong()

    override fun instances(): Sequence<TestInstance<Index<K>>> = indexes.hashIndexes()

    companion object {

        @JvmStatic
        internal val indexes = Indexes()
    }

}

internal class LongHashIndexTest: HashIndexTest<Long>() {

    override fun nextKey() = uniqueGenerator.getAndIncrement()

}

internal class StringHashIndexTest: HashIndexTest<String>() {

    override fun nextKey() = uniqueGenerator.getAndIncrement().toString()

}
