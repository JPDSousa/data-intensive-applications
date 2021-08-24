package org.example.log

import org.example.TestGenerator
import org.example.TestInstance
import org.example.encoder.Encoders
import org.koin.dsl.module


val logFactoriesModule = module {

    single<StringLogFactories>(lineLogQ) {
        LineLogFactories()
    }

    single<StringLogFactories> {
        get(lineLogQ)
    }

    single<ByteArrayLogFactories>(binaryLogQ) { ByteArrayLogFactoriesimpl() }

    single<ByteArrayLogFactories> {
        get(binaryLogQ)
    }
}

interface LogFactories<T>: TestGenerator<LogFactory<T>>
interface StringLogFactories: LogFactories<String>
interface ByteArrayLogFactories: LogFactories<ByteArray>

private class LineLogFactories: StringLogFactories {

    override fun generate() = sequenceOf(TestInstance("String Log Factory") {
        LineLogFactory()
    })

}

private class ByteArrayLogFactoriesimpl: ByteArrayLogFactories {

    override fun generate() = sequenceOf(
        TestInstance("Binary Log Factory") {
            BinaryLogFactory()
        })

}

class LogEncoderFactories<S, T>(
    private val encoders: Encoders<S, T>,
    private val factories: LogFactories<T>
): LogFactories<S> {

    override fun generate(): Sequence<TestInstance<LogFactory<S>>> = sequence {

        for (factory in factories.generate()) {

            for (encoder in encoders.generate()) {

                yield(TestInstance("Log Encoder with string encoder") {
                    LogEncoderFactory(factory.instance(), encoder.instance())
                })
            }

        }
    }
}
