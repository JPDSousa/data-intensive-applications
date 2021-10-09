package org.example.log

import org.example.TestGenerator
import org.example.TestInstance
import org.example.encoder.Encoders
import org.example.encoder.StringStringMapEntry2StringEncoders
import org.koin.dsl.module


val logFactoriesModule = module {

    single<StringLogFactories>(lineLogQ) {
        LineLogFactories()
    }

    single<StringLogFactories> {
        get(lineLogQ)
    }

    single<ByteArrayLogFactories>(binaryLogQ) { BinaryLogFactories() }

    single<ByteArrayLogFactories> {
        get(binaryLogQ)
    }

    single<StringStringMapEntryLogFactories> {
        StringStringMapEntryLogFactoriesDelegate(
            LogEncoderFactories(
                get<StringStringMapEntry2StringEncoders>(),
                get<StringLogFactories>()
            )
        )
    }
}

interface LogFactories<T>: TestGenerator<LogFactory<T>>
interface StringLogFactories: LogFactories<String>
interface ByteArrayLogFactories: LogFactories<ByteArray>
interface StringStringMapEntryLogFactories: LogFactories<Map.Entry<String, String>>

private class StringStringMapEntryLogFactoriesDelegate(private val delegate: LogFactories<Map.Entry<String, String>>)
    : StringStringMapEntryLogFactories, LogFactories<Map.Entry<String, String>> by delegate

private class LineLogFactories: StringLogFactories {

    override fun generate() = sequenceOf(TestInstance("String Log Factory") {
        LineLogFactory()
    })

}

private class BinaryLogFactories: ByteArrayLogFactories {

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

        for (factory in factories) {

            for (encoder in encoders) {

                yield(TestInstance("Log Encoder with string encoder") {
                    LogEncoderFactory(factory.instance(), encoder.instance())
                })
            }

        }
    }
}
