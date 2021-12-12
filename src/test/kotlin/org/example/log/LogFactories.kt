package org.example.log

import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.arbitrary.bind
import org.example.GenWrapper
import org.example.encoder.*
import org.example.getAllGen
import org.example.map
import org.example.trash.PathTrashes
import org.koin.core.module.Module
import org.koin.core.qualifier.Qualifier
import org.koin.core.scope.Scope
import org.koin.dsl.module

data class StringLogFactories(
    override val gen: Gen<LogFactoryB<String>>
) : GenWrapper<LogFactoryB<String>>
data class ByteArrayLogFactories(
    override val gen: Gen<LogFactoryB<ByteArray>>
) : GenWrapper<LogFactoryB<ByteArray>>

data class StringStringMapEntryLogFactories(
    override val gen: Gen<EntryLogFactory<String, String>>
) : GenWrapper<EntryLogFactory<String, String>>
data class LongByteArrayMapEntryLogFactories(
    override val gen: Gen<EntryLogFactory<Long, ByteArray>>
) : GenWrapper<EntryLogFactory<Long, ByteArray>>

// TODO generate delegates in different order
internal inline fun <T, S, reified TFG, reified SFG, reified E> Module.registerLogGenerators(
    baseQualifier: Qualifier,
    typeQualifiers: Iterable<Qualifier>,
    targetEncodingQualifiers: Iterable<Qualifier>,
    crossinline baseFactory: Scope.() -> Gen<LogFactoryB<T>>,
    crossinline baseDelegate: (Gen<LogFactoryB<T>>) -> TFG,
) where TFG: GenWrapper<LogFactoryB<T>>,
        SFG: GenWrapper<LogFactoryB<S>>,
        E: GenWrapper<Encoder<T, S>> {

    single(baseQualifier) { baseDelegate(baseFactory()) }

    val logEncoderQualifiers = targetEncodingQualifiers - logEncoderQ
    single(logEncoderQ) { baseDelegate(
        logEncoderFactories(
            get<E>().gen,
            getAllGen<LogFactoryB<S>, SFG>(logEncoderQualifiers)
        )
    ) }

    val sizeCachedQualifiers = typeQualifiers - delegateQualifiers
    single(sizeLogQ) { baseDelegate(
        getAllGen<LogFactoryB<T>, TFG>(sizeCachedQualifiers).map {
            SizeCachedLogFactory(it) as LogFactoryB<T> /* = org.example.log.LogFactory<T, org.example.log.Log<T>> */
        }
    ) }

    single { baseDelegate(getAllGen<LogFactoryB<T>, TFG>(typeQualifiers)) }
}

fun <S, T> logEncoderFactories(
    encoders: Gen<Encoder<S, T>>,
    factories: Gen<LogFactoryB<T>>
): Gen<LogFactoryB<S>> = Arb.bind(encoders, factories) { encoder, factory ->
    LogEncoderFactory(factory, encoder) as LogFactoryB<S>
}

val logFactoriesModule = module {

    registerLogGenerators<String, ByteArray, StringLogFactories, ByteArrayLogFactories, StringByteArrayEncoders>(
        lineLogQ,
        stringQualifiers,
        byteArrayQualifiers,
        { get<PathTrashes>().gen.map { LineLogFactory(it) as LogFactoryB<String> /* = org.example.log.LogFactory<kotlin.String, org.example.log.Log<kotlin.String>> */ } },
        { StringLogFactories(it) },
    )

    registerLogGenerators<ByteArray, String, ByteArrayLogFactories, StringLogFactories, ByteArrayStringEncoders>(
        binaryLogQ,
        byteArrayQualifiers,
        stringQualifiers,
        { get<PathTrashes>().gen.map { BinaryLogFactory(it) as LogFactoryB<ByteArray> /* = org.example.log.LogFactory<kotlin.ByteArray, org.example.log.Log<kotlin.ByteArray>> */ } },
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
