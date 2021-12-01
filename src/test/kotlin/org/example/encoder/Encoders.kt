package org.example.encoder

import io.kotest.property.Gen
import io.kotest.property.exhaustive.exhaustive
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import org.example.GenWrapper
import org.example.InstantSerializer
import org.koin.dsl.module
import java.time.Instant

@ExperimentalSerializationApi
val encoderModule = module {

    single { Instant2StringEncoders(jsonEncoderGen(InstantSerializer())) }

    single { Instant2ByteArrayEncoders(protobufEncoderGen(InstantSerializer())) }

    single { StringStringMapEntry2StringEncoders(jsonEncoderGen(serializer())) }

    single { LongByteArrayMapEntry2StringEncoders(jsonEncoderGen(serializer())) }

    single { StringStringMapEntry2ByteArrayEncoders(protobufEncoderGen(serializer())) }

    single { LongByteArrayMapEntry2ByteArrayEncoders(protobufEncoderGen(serializer())) }
}

fun <S> jsonEncoderGen(serializer: KSerializer<S>): Gen<Encoder<S, String>>
        = listOf(JsonStringEncoder(serializer)).exhaustive()

@ExperimentalSerializationApi
fun <S> protobufEncoderGen(serializer: KSerializer<S>): Gen<Encoder<S, ByteArray>>
        = listOf(ProtobufBinaryEncoder(serializer)).exhaustive()

data class Instant2StringEncoders(
    override val gen: Gen<Encoder<Instant, String>>
): GenWrapper<Encoder<Instant, String>>

data class Instant2ByteArrayEncoders(
    override val gen: Gen<Encoder<Instant, ByteArray>>
): GenWrapper<Encoder<Instant, ByteArray>>

data class StringStringMapEntry2StringEncoders(
    override val gen: Gen<Encoder<Map.Entry<String, String>, String>>
) : GenWrapper<Encoder<Map.Entry<String, String>, String>>
data class LongByteArrayMapEntry2StringEncoders(
    override val gen: Gen<Encoder<Map.Entry<Long, ByteArray>, String>>
) : GenWrapper<Encoder<Map.Entry<Long, ByteArray>, String>>
data class StringStringMapEntry2ByteArrayEncoders(
    override val gen: Gen<Encoder<Map.Entry<String, String>, ByteArray>>
) : GenWrapper<Encoder<Map.Entry<String, String>, ByteArray>>
data class LongByteArrayMapEntry2ByteArrayEncoders(
    override val gen: Gen<Encoder<Map.Entry<Long, ByteArray>, ByteArray>>
) : GenWrapper<Encoder<Map.Entry<Long, ByteArray>, ByteArray>>
