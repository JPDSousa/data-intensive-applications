package org.example

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import org.example.kv.KeyValueEntry
import java.io.IOException
import java.nio.file.Path
import java.nio.file.attribute.FileAttribute
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.createFile

fun possiblyArrayEquals(val1: Any?, val2: Any?): Boolean {

    if (val1 is ByteArray && val2 is ByteArray) {
        return val1.contentEquals(val2)
    }

    return val1 == val2
}

class InstantSerializer : KSerializer<Instant> {

    override val descriptor = PrimitiveSerialDescriptor("InstantEpochAsMillis", PrimitiveKind.LONG)

    override fun deserialize(decoder: Decoder): Instant = Instant.ofEpochMilli(decoder.decodeLong())

    override fun serialize(encoder: Encoder, value: Instant) = encoder.encodeLong(value.toEpochMilli())
}

@Serializable(with = DataEntrySerializer::class)
data class DataEntry<K, V>(override val key: K, override val value: V) : Map.Entry<K, V>

fun <K, V> Sequence<DataEntry<K, V>>.toMap() = associate { Pair(it.key, it.value) }

class DataEntrySerializer<K, V>(private val keySerializer: KSerializer<K>,
                                private val valueSerializer: KSerializer<V>): KSerializer<DataEntry<K, V>> {

    override fun deserialize(decoder: Decoder): DataEntry<K, V> = decoder.decodeStructure(descriptor) {

        val key = keySerializer.deserialize(decoder)
        val value = valueSerializer.deserialize(decoder)

        DataEntry(key, value)
    }

    override fun serialize(encoder: Encoder, value: DataEntry<K, V>) {
        encoder.encodeStructure(descriptor) {
            keySerializer.serialize(encoder, value.key)
            valueSerializer.serialize(encoder, value.value)
        }
    }

    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("DataEntry") {
        element("key", keySerializer.descriptor)
        element("value", valueSerializer.descriptor)
    }

}
