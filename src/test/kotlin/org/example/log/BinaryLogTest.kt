package org.example.log

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.ApplicationTest
import org.example.TestResources
import org.example.generator.ByteArrayGenerator
import java.util.concurrent.atomic.AtomicLong

internal class BinaryLogTest: ApplicationTest(), LogTest<ByteArray> {

    private val valueIterator = byteArrayGenerator.generate().iterator()

    override fun instances() = generator.generate()

    override fun nextValue(): ByteArray = when {
        valueIterator.hasNext() -> valueIterator.next()
        else -> throw NoSuchElementException("No more values")
    }

    companion object {

        @JvmStatic
        private val byteArrayGenerator: ByteArrayGenerator = application.koin.get()

        @JvmStatic
        private val generator: BinaryLogs = application.koin.get(binaryLogQ)

    }

}

internal class BinaryLogFactoryTest: ApplicationTest(), LogFactoryTest<ByteArray> {

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
        private val logFactories: ByteArrayLogFactories = application.koin.get(binaryLogQ)

    }
}
