package org.example.log

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.TestResources
import org.example.application
import org.example.generator.ByteArrayGenerator
import org.junit.jupiter.api.AfterAll
import java.util.concurrent.atomic.AtomicLong

internal class BinaryLogTest: LogTest<ByteArray> {

    private val valueIterator = byteArrayGenerator.generate().iterator()

    override fun instances() = generator.generate()

    override fun nextValue(): ByteArray = when {
        valueIterator.hasNext() -> valueIterator.next()
        else -> throw NoSuchElementException("No more values")
    }

    companion object {

        @JvmStatic
        private val application = application()

        @JvmStatic
        private val byteArrayGenerator: ByteArrayGenerator = application.koin.get()

        @JvmStatic
        private val generator: BinaryLogs = application.koin.get(binaryLogQ)

        @JvmStatic
        @AfterAll
        fun closeResources() {
            application.close()
        }

    }

}

internal class BinaryLogFactoryTest: LogFactoryTest<ByteArray> {

    @ExperimentalSerializationApi
    override fun instances() = logFactories.generate()

    override val resources: TestResources
        get() = application.koin.get()

    private val uniqueGenerator = AtomicLong()

    override fun nextValue(): ByteArray = uniqueGenerator.getAndIncrement()
        .toString()
        .toByteArray()

    companion object {

        @JvmStatic
        private val application = application()

        @JvmStatic
        private val logFactories: ByteArrayLogFactories = application.koin.get(binaryLogQ)

        @JvmStatic
        @AfterAll
        fun closeResources() {
            application.close()
        }
    }
}
