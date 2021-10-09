package org.example.log

import org.example.TestGenerator
import org.example.TestInstance
import org.example.TestResources
import org.example.generator.ByteArrayGenerator
import org.example.generator.Generator
import org.example.generator.StringGenerator
import org.koin.dsl.module

val logsModule = module {

    single<StringLogs>(lineLogQ) {
        StringLogsDelegate(
            LogGenerator(
                get(),
                get<StringLogFactories>(lineLogQ),
                get<StringGenerator>()
            )
        )
    }

    single<BinaryLogs>(binaryLogQ) {
        BinaryLogsDelegate(
            LogGenerator(
                get(),
                get<ByteArrayLogFactories>(binaryLogQ),
                get<ByteArrayGenerator>()
            )
        )
    }

}

interface StringLogs: TestGenerator<Log<String>>
interface BinaryLogs: TestGenerator<Log<ByteArray>>

private class StringLogsDelegate(private val generator: LogGenerator<String>): StringLogs,
    TestGenerator<Log<String>> by generator

private class BinaryLogsDelegate(private val generator: LogGenerator<ByteArray>): BinaryLogs,
    TestGenerator<Log<ByteArray>> by generator


class LogGenerator<T>(
    private val resources: TestResources,
    private val factories: LogFactories<T>,
    private val valueGenerator: Generator<T>
): TestGenerator<Log<T>> {

    override fun generate(): Sequence<TestInstance<Log<T>>> = factories.generate().flatMap {
        sequence {
            for (size in testableSizes) {
                yield(TestInstance("Log with initial size $size ~ $it") {
                    // TODO candidate for function extraction
                    it.instance().create(resources.allocateTempLogFile())
                        .also { log ->
                            valueGenerator.generate()
                                .take(size)
                                .forEach { log.append(it) }
                        }
                })
            }
        }
    }

    companion object {

        val testableSizes = listOf(0, 1, 10)
    }
}
