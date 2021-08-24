package org.example.encoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import org.example.InstantSerializer
import org.example.TestGenerator
import org.example.TestInstance
import org.koin.dsl.module
import java.time.Instant

val encodersModule = module {
    single<Instant2StringEncoders> {
        DelegateInstant2StringEncoders(
            StringEncoderGenerator(InstantSerializer())
        )
    }
    single<Instant2ByteArrayEncoders> {
        DelegateInstant2ByteArrayEncoders(
            ProtobufEncoderGenerator(InstantSerializer())
        )
    }

    single<StringStringMapEntry2StringEncoders> {
        DelegateStringStringMapEntry2StringEncoders(
            StringEncoderGenerator(serializer())
        )
    }

    single<LongByteArrayMapEntry2StringEncoders> {
        DelegateLongByteArrayMapEntry2StringEncoders(
            StringEncoderGenerator(serializer())
        )
    }

    single<StringStringMapEntry2ByteArrayEncoders> {
        DelegateStringStringMapEntry2ByteArrayEncoders(
            ProtobufEncoderGenerator(serializer())
        )
    }

    single<LongByteArrayMapEntry2ByteArrayEncoders> {
        DelegateLongByteArrayMapEntry2ByteArrayEncoders(
            ProtobufEncoderGenerator(serializer())
        )
    }
}

interface Encoders<S, T>: TestGenerator<Encoder<S, T>>

interface Instant2StringEncoders: Encoders<Instant, String>
interface Instant2ByteArrayEncoders: Encoders<Instant, ByteArray>

private class DelegateInstant2StringEncoders(private val delegate: Encoders<Instant, String>)
    : Instant2StringEncoders, Encoders<Instant, String> by delegate

private class DelegateInstant2ByteArrayEncoders(private val delegate: Encoders<Instant, ByteArray>)
    : Instant2ByteArrayEncoders, Encoders<Instant, ByteArray> by delegate

interface StringStringMapEntry2StringEncoders: Encoders<Map.Entry<String, String>, String>
interface LongByteArrayMapEntry2StringEncoders: Encoders<Map.Entry<Long, ByteArray>, String>
interface StringStringMapEntry2ByteArrayEncoders: Encoders<Map.Entry<String, String>, ByteArray>
interface LongByteArrayMapEntry2ByteArrayEncoders: Encoders<Map.Entry<Long, ByteArray>, ByteArray>

private class DelegateStringStringMapEntry2StringEncoders(
    private val delegate: Encoders<Map.Entry<String, String>, String>)
    : StringStringMapEntry2StringEncoders, Encoders<Map.Entry<String, String>, String> by delegate

private class DelegateLongByteArrayMapEntry2StringEncoders(
    private val delegate: Encoders<Map.Entry<Long, ByteArray>, String>)
    : LongByteArrayMapEntry2StringEncoders, Encoders<Map.Entry<Long, ByteArray>, String> by delegate

private class DelegateStringStringMapEntry2ByteArrayEncoders(
    private val delegate: Encoders<Map.Entry<String, String>, ByteArray>)
    : StringStringMapEntry2ByteArrayEncoders, Encoders<Map.Entry<String, String>, ByteArray> by delegate

private class DelegateLongByteArrayMapEntry2ByteArrayEncoders(
    private val delegate: Encoders<Map.Entry<Long, ByteArray>, ByteArray>)
    : LongByteArrayMapEntry2ByteArrayEncoders, Encoders<Map.Entry<Long, ByteArray>, ByteArray> by delegate

class StringEncoderGenerator<S>(private val serializer: KSerializer<S>): Encoders<S, String> {

    override fun generate() = sequenceOf<TestInstance<Encoder<S, String>>>(
        TestInstance("Json encoder") { JsonStringEncoder(serializer) }
    )
}

class ProtobufEncoderGenerator<S>(private val serializer: KSerializer<S>): Encoders<S, ByteArray> {

    @ExperimentalSerializationApi
    override fun generate() = sequenceOf(
        TestInstance("Protobuf encoder") { ProtobufBinaryEncoder(serializer) }
    )
}

private class ByteArrayStringEncoderGenerator: TestGenerator<Encoder<String, ByteArray>> {

    override fun generate() = sequenceOf(
        TestInstance("native string to byte encoder") { ByteArrayStringEncoder() }
    )
}

