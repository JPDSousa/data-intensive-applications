package org.example.log

import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.arbitrary.bind
import io.kotest.property.exhaustive.exhaustive
import org.example.GenWrapper
import org.example.encoder.Encoder
import org.example.encoder.LongByteArrayMapEntry2StringEncoders
import org.example.encoder.StringStringMapEntry2StringEncoders
import org.koin.dsl.module


val logFactoriesModule = module {

    single(lineLogQ) { StringLogFactories(listOf(LineLogFactory).exhaustive()) }

    single<StringLogFactories> { get(lineLogQ) }

    single(binaryLogQ) { ByteArrayLogFactories(listOf(BinaryLogFactory).exhaustive()) }

    single<ByteArrayLogFactories> { get(binaryLogQ) }

    single { StringStringMapEntryLogFactories(
        logEncoderFactories(
            get<StringStringMapEntry2StringEncoders>().gen,
            get<StringLogFactories>().gen
        )
    ) }

    single { LongByteArrayMapEntryLogFactories(
        logEncoderFactories(
            get<LongByteArrayMapEntry2StringEncoders>().gen,
            get<StringLogFactories>().gen
        )
    ) }
}

data class StringLogFactories(
    override val gen: Gen<LogFactory<String>>
) : GenWrapper<LogFactory<String>>
data class ByteArrayLogFactories(
    override val gen: Gen<LogFactory<ByteArray>>
) : GenWrapper<LogFactory<ByteArray>>

data class StringStringMapEntryLogFactories(
    override val gen: Gen<LogFactory<Map.Entry<String, String>>>
) : GenWrapper<LogFactory<Map.Entry<String, String>>>
data class LongByteArrayMapEntryLogFactories(
    override val gen: Gen<LogFactory<Map.Entry<Long, ByteArray>>>
) : GenWrapper<LogFactory<Map.Entry<Long, ByteArray>>>

fun <S, T> logEncoderFactories(
    encoders: Gen<Encoder<S, T>>,
    factories: Gen<LogFactory<T>>
): Gen<LogFactory<S>> = Arb.bind(encoders, factories) { encoder, factory ->
    LogEncoderFactory(factory, encoder)
}
