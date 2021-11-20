package org.example.log

import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.arbitrary.bind
import io.kotest.property.exhaustive.exhaustive
import org.example.GenWrapper
import org.example.encoder.*
import org.example.getAllGen
import org.example.map
import org.koin.core.module.Module
import org.koin.core.qualifier.Qualifier
import org.koin.dsl.module

// TODO generate delegates in different order
internal inline fun <T, S, reified TFG, reified SFG, reified E> Module.registerLogGenerators(
    baseQualifier: Qualifier,
    typeQualifiers: Iterable<Qualifier>,
    targetEncodingQualifiers: Iterable<Qualifier>,
    crossinline baseFactory: () -> Gen<LogFactory<T>>,
    crossinline baseDelegate: (Gen<LogFactory<T>>) -> TFG,
)
        where TFG: GenWrapper<LogFactory<T>>,
              SFG: GenWrapper<LogFactory<S>>,
              E: GenWrapper<Encoder<T, S>> {

    single(baseQualifier) { baseDelegate(baseFactory()) }

    val logEncoderQualifiers = targetEncodingQualifiers - logEncoderQ
    single(logEncoderQ) { baseDelegate(
        logEncoderFactories(
            get<E>().gen,
            getAllGen<LogFactory<S>, SFG>(logEncoderQualifiers)
        )
    ) }

    val sizeCachedQualifiers = typeQualifiers - delegateQualifiers
    single(sizeLogQ) { baseDelegate(
        getAllGen<LogFactory<T>, TFG>(sizeCachedQualifiers).map {
            SizeCachedLogFactory(it)
        }
    ) }

    single { baseDelegate(getAllGen<LogFactory<T>, TFG>(typeQualifiers)) }
}

val logFactoriesModule = module {

    registerLogGenerators<String, ByteArray, StringLogFactories, ByteArrayLogFactories, StringByteArrayEncoders>(
        lineLogQ,
        stringQualifiers,
        byteArrayQualifiers,
        { listOf(LineLogFactory).exhaustive() },
        { StringLogFactories(it) },
    )

    registerLogGenerators<ByteArray, String, ByteArrayLogFactories, StringLogFactories, ByteArrayStringEncoders>(
        binaryLogQ,
        byteArrayQualifiers,
        stringQualifiers,
        { listOf(BinaryLogFactory).exhaustive() },
        { ByteArrayLogFactories(it) }
    )

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
