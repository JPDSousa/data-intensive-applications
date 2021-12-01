package org.example.index

import io.kotest.common.DelicateKotest
import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.distinct
import io.kotest.property.arbitrary.map
import io.kotest.property.arbitrary.string
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.serializer
import org.example.GenWrapper
import org.example.encoder.Encoder
import org.example.encoder.jsonEncoderGen
import org.example.encoder.protobufEncoderGen
import org.example.log.ByteArrayLogFactories
import org.example.log.LogFactory
import org.example.log.StringLogFactories
import org.example.log.logEncoderFactories
import org.example.merge
import org.koin.core.module.Module
import org.koin.dsl.module

@DelicateKotest
@ExperimentalSerializationApi
val indexesModule = module {

    single { StringIndexEntryLogFactories(
        logEncoderFactories(get<StringStringEncoders>().gen, get<StringLogFactories>().gen).merge(
            logEncoderFactories(get<StringByteArrayEncoders>().gen, get<ByteArrayLogFactories>().gen)
        )
    ) }

    single { LongIndexEntryLogFactories(
        logEncoderFactories(get<LongStringEncoders>().gen, get<StringLogFactories>().gen).merge(
            logEncoderFactories(get<LongByteArrayEncoders>().gen, get<ByteArrayLogFactories>().gen)
        )
    ) }

    single { StringStringEncoders(jsonEncoderGen(serializer())) }
    single { StringByteArrayEncoders(protobufEncoderGen(serializer())) }
    single { LongStringEncoders(jsonEncoderGen(serializer())) }
    single { LongByteArrayEncoders(protobufEncoderGen(serializer())) }

    registerIndices<String, StringIndexFactories, StringIndices> { StringIndices(it) }
    registerIndices<Long, LongIndexFactories, LongIndices> { LongIndices(it) }
}

@DelicateKotest
private inline fun <K, reified GF, reified GK> Module.registerIndices(
    crossinline genWrapperSupplier: (Gen<Index<K>>) -> GK
) where GF : GenWrapper<IndexFactory<K>>,
        GK: GenWrapper<Index<K>> {
    single { genWrapperSupplier(indexFromFactory(get<GF>().gen)) }
    for (indexQ in indicesQ) {
        single(indexQ) { genWrapperSupplier(indexFromFactory(get<GF>(indexQ).gen)) }
    }
}

private val indexInstanceName = "${Index::class.simpleName} from ${HashIndexFactory::class.simpleName}"
@DelicateKotest
private val indexNameArb = Arb.string().distinct().map { "$indexInstanceName (${it})"}
@DelicateKotest
private fun <K> indexFromFactory(gen: Gen<IndexFactory<K>>) = Arb.bind(gen, indexNameArb) { factory, indexName ->
    factory.create(indexName)
}

data class StringIndexEntryLogFactories(
    override val gen: Gen<LogFactory<IndexEntry<String>>>
) : GenWrapper<LogFactory<IndexEntry<String>>>
data class LongIndexEntryLogFactories(
    override val gen: Gen<LogFactory<IndexEntry<Long>>>
) : GenWrapper<LogFactory<IndexEntry<Long>>>

data class StringStringEncoders(
    override val gen: Gen<Encoder<IndexEntry<String>, String>>
) : GenWrapper<Encoder<IndexEntry<String>, String>>
data class StringByteArrayEncoders(
    override val gen: Gen<Encoder<IndexEntry<String>, ByteArray>>
) : GenWrapper<Encoder<IndexEntry<String>, ByteArray>>
data class LongStringEncoders(
    override val gen: Gen<Encoder<IndexEntry<Long>, String>>
) : GenWrapper<Encoder<IndexEntry<Long>, String>>
data class LongByteArrayEncoders(
    override val gen: Gen<Encoder<IndexEntry<Long>, ByteArray>>
) : GenWrapper<Encoder<IndexEntry<Long>, ByteArray>>

data class StringIndices(override val gen: Gen<Index<String>>) : GenWrapper<Index<String>>
data class LongIndices(override val gen: Gen<Index<Long>>) : GenWrapper<Index<Long>>

