package org.example.encoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.protobuf.ProtoBuf
import org.koin.core.qualifier.named
import java.nio.charset.Charset

@ExperimentalSerializationApi
val protobufQualifier = named(ProtobufBinaryEncoder::class.qualifiedName!!)
val jsonQualifier = named(JsonStringEncoder::class.qualifiedName!!)
val charsetQualifier = named(CharsetEncoder::class.qualifiedName!!)


interface Encoder<S, T> {

    fun encode(source: S): T

    fun decode(target: T): S

}

enum class SupportedCharsets(val charset: Charset) {

    UTF_8(Charsets.UTF_8),
    UTF_16(Charsets.UTF_16),
    US_ASCII(Charsets.US_ASCII)

}

class CharsetEncoder(private val charset: SupportedCharsets = SupportedCharsets.UTF_8): Encoder<String, ByteArray> {

    override fun encode(source: String) = source.toByteArray(charset.charset)

    override fun decode(target: ByteArray) = target.toString(charset.charset)
}

class JsonStringEncoder<S>(private val serializer: KSerializer<S>): Encoder<S, String> {

    override fun encode(source: S): String {
        return Json.encodeToString(serializer, source)
    }

    override fun decode(target: String): S {
        return Json.decodeFromString(serializer, target)
    }
}

@ExperimentalSerializationApi
class ProtobufBinaryEncoder<S>(private val serializer: KSerializer<S>): Encoder<S, ByteArray> {

    override fun encode(source: S): ByteArray = ProtoBuf.encodeToByteArray(serializer, source)

    override fun decode(target: ByteArray): S = ProtoBuf.decodeFromByteArray(serializer, target)
}
