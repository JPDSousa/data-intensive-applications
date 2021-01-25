package org.example.encoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import org.example.TestInstance

class Encoders {

    fun <S> strings(serializer: KSerializer<S>): Sequence<TestInstance<Encoder<S, String>>> = sequenceOf(
            TestInstance("Json encoder") { JsonStringEncoder(serializer) }
    )

    @ExperimentalSerializationApi
    fun <S> binaries(serializer: KSerializer<S>): Sequence<TestInstance<Encoder<S, ByteArray>>> = sequenceOf(
            TestInstance("Protobuf encoder") { ProtobufBinaryEncoder(serializer) }
    )

    @ExperimentalSerializationApi
    fun stringToBinaries(): Sequence<TestInstance<Encoder<String, ByteArray>>> = binaries(serializer<String>()) +
            sequenceOf(TestInstance("native string to byte encoder") { ByteArrayStringEncoder() })

}
