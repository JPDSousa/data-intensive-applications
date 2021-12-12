package org.example.log

import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.arbitrary.*
import io.kotest.property.exhaustive.exhaustive
import org.example.GenWrapper
import org.example.TestResources
import org.koin.dsl.module

val logsModule = module {

    single(lineLogQ) { StringLogs(logs(
        get(),
        get<StringLogFactories>(lineLogQ).gen,
        Arb.string()
    )) }

    single(binaryLogQ) { BinaryLogs(logs(
        get(),
        get<ByteArrayLogFactories>(binaryLogQ).gen,
        Arb.byteArray((1..10).toList().exhaustive(), Arb.byte())
    ))}

}

data class StringLogs(
    override val gen: Gen<Log<String>>
) : GenWrapper<Log<String>>
data class BinaryLogs(
    override val gen: Gen<Log<ByteArray>>
) : GenWrapper<Log<ByteArray>>

private val testableSizes = listOf(0, 1, 10).exhaustive()

private fun <T> logs(
    resources: TestResources,
    factories: Gen<LogFactoryB<T>>,
    valueGenerator: Arb<T>
) = Arb.bind(
    factories,
    testableSizes
) { factory, size ->
    factory.create(resources.allocateTempLogFile())
        .also { log ->
            (0..size).map { valueGenerator.next() }
                .forEach(log::append)
        }
}
