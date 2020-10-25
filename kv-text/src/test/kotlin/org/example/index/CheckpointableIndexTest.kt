package org.example.index

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.TestInstance
import org.example.log.Index
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.util.concurrent.atomic.AtomicLong

internal abstract class AbstractCheckpointableIndexTest<K>: IndexTest<K> {

    internal var indexes: Indexes? = null
    internal val uniqueGenerator = AtomicLong()

    @BeforeEach fun openResources() {
        indexes = Indexes()
    }

    @AfterEach fun closeResources() {
        indexes!!.close()
    }

}

internal class CheckpointableStringIndexTest: AbstractCheckpointableIndexTest<String>() {

    @ExperimentalSerializationApi
    override fun instances(): Sequence<TestInstance<Index<String>>> = indexes!!.instances()

    override fun nextKey() = uniqueGenerator.getAndIncrement().toString()

}

internal class CheckpointableBinaryIndexTest: AbstractCheckpointableIndexTest<ByteArray>() {

    @ExperimentalSerializationApi
    override fun instances(): Sequence<TestInstance<Index<ByteArray>>> = indexes!!.instances()

    override fun nextKey() = uniqueGenerator.getAndIncrement()
            .toString()
            .toByteArray()

}
