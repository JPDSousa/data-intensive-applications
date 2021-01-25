package org.example.log

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.TestInstance
import org.example.TestResources

class Logs(private val resources: TestResources, private val logFactories: LogFactories) {

    fun lineLogInstances() = logFactories.lineLogInstances().map {
        TestInstance(it.name) {
            it.instance().create(resources.allocateTempFile("log-", ".csv"))
        }
    }

    @ExperimentalSerializationApi
    fun stringEncodedInstances(): Sequence<TestInstance<Log<String>>> = logFactories.stringEncodedInstances().map {
        TestInstance(it.name) {
            it.instance().create(resources.allocateTempFile("log-", ".log"))
        }
    }

    @ExperimentalSerializationApi
    fun stringInstances() = lineLogInstances() + stringEncodedInstances()

    fun binaryInstances() = logFactories.binaryLogInstances().map {
        TestInstance(it.name) {
            it.instance().create(resources.allocateTempFile("log-", ".bin"))
        }
    }

}
