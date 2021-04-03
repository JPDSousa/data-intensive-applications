package org.example

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure

fun possiblyArrayEquals(val1: Any?, val2: Any?): Boolean {

    if (val1 is ByteArray && val2 is ByteArray) {
        return val1.contentEquals(val2)
    }

    return val1 == val2
}

@Serializable(with = DataEntrySerializer::class)
data class DataEntry<K, V>(override val key: K, override val value: V) : Map.Entry<K, V>

class DataEntrySerializer<K, V>(private val keySerializer: KSerializer<K>,
                                private val valueSerializer: KSerializer<V>) : KSerializer<Map.Entry<K, V>> {

    override fun deserialize(decoder: Decoder): Map.Entry<K, V> = decoder.decodeStructure(descriptor) {

        val key = keySerializer.deserialize(decoder)
        val value = valueSerializer.deserialize(decoder)

        DataEntry(key, value)
    }

    override fun serialize(encoder: Encoder, value: Map.Entry<K, V>) {
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
