package org.example.log

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.serializer
import org.example.TestInstance
import org.example.encoder.Encoders

class LogFactories(private val encoders: Encoders) {

    @ExperimentalSerializationApi
    fun binaryInstances(): Sequence<TestInstance<LogFactory<ByteArray>>>
            = binaryLogInstances() + binaryEncodedInstances()

    fun binaryLogInstances(): Sequence<TestInstance<LogFactory<ByteArray>>> = sequenceOf(
            TestInstance("Binary Log Factory") {
                BinaryLogFactory()
            })

    @ExperimentalSerializationApi
    fun binaryEncodedInstances(): Sequence<TestInstance<LogFactory<ByteArray>>> = sequence {

        for (encoder in encoders.strings(serializer<ByteArray>())) {
            yield(TestInstance("Log Encoder with string encoder") {
                LogEncoderFactory(LineLogFactory(), encoder.instance())
            })
        }

        for (encoder in encoders.binaries(serializer<ByteArray>())) {
            yield(TestInstance("Log Encoder with binary encoder") {
                LogEncoderFactory(BinaryLogFactory(), encoder.instance())
            })
        }
    }

    @ExperimentalSerializationApi
    fun stringInstances(): Sequence<TestInstance<LogFactory<String>>> = lineLogInstances() + stringEncodedInstances()

    fun lineLogInstances(): Sequence<TestInstance<LogFactory<String>>> = sequenceOf(TestInstance("String Log Factory") {
        LineLogFactory()
    })

    @ExperimentalSerializationApi
    fun stringEncodedInstances(): Sequence<TestInstance<LogFactory<String>>> = sequence {

        for (encoder in encoders.strings(serializer<String>())) {
            yield(TestInstance("Log Encoder with string encoder") {
                LogEncoderFactory(LineLogFactory(), encoder.instance())
            })
        }

        for (encoder in encoders.stringToBinaries()) {
            yield(TestInstance("Log encoder with binary encoder") {
                LogEncoderFactory(BinaryLogFactory(), encoder.instance())
            })
        }
    }

}
