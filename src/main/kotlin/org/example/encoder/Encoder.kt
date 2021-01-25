package org.example.encoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.protobuf.ProtoBuf
import java.nio.charset.Charset
import kotlin.text.Charsets.UTF_8

interface Encoder<S, T> {

    fun encode(source: S): T

    fun decode(target: T): S

}

class ByteArrayStringEncoder(private val charset: Charset = UTF_8): Encoder<String, ByteArray> {

    override fun encode(source: String) = source.toByteArray(charset)

    override fun decode(target: ByteArray) = target.toString(charset)
}

class JsonStringEncoder<S>(private val serializer: KSerializer<S>): Encoder<S, String> {

    override fun encode(source: S): String = Json.encodeToString(serializer, source)

    override fun decode(target: String): S = Json.decodeFromString(serializer, target)
}

@ExperimentalSerializationApi
class ProtobufBinaryEncoder<S>(private val serializer: KSerializer<S>): Encoder<S, ByteArray> {

    override fun encode(source: S): ByteArray = ProtoBuf.encodeToByteArray(serializer, source)

    override fun decode(target: ByteArray): S = ProtoBuf.decodeFromByteArray(serializer, target)
}
