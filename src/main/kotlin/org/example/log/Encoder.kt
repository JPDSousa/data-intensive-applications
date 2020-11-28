package org.example.log

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.json.Json
import kotlinx.serialization.protobuf.ProtoBuf

interface Encoder<S, T> {

    fun encode(source: S): T

    fun decode(target: T): S

}

class JsonStringEncoder<S>(private val serializer: SerializationStrategy<S>,
                           private val deserializer: DeserializationStrategy<S>): Encoder<S, String> {

    override fun encode(source: S): String = Json.encodeToString(serializer, source)

    override fun decode(target: String): S = Json.decodeFromString(deserializer, target)
}

@ExperimentalSerializationApi
class ProtobufBinaryEncoder<S>(private val serializer: SerializationStrategy<S>,
                               private val deserializer: DeserializationStrategy<S>): Encoder<S, ByteArray> {

    override fun encode(source: S): ByteArray = ProtoBuf.encodeToByteArray(serializer, source)

    override fun decode(target: ByteArray): S = ProtoBuf.decodeFromByteArray(deserializer, target)
}
