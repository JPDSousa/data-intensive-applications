package org.example

import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import java.util.stream.Stream
import kotlin.streams.asStream

data class TestInstance<out T>(val name: String, val instance: () -> T) {

    fun asDynamicTest(executable: (T) -> Unit): DynamicTest = dynamicTest(name) {
        executable(instance())
    }
}

fun <T> Sequence<TestInstance<T>>.test(executable: (T) -> Unit): Stream<DynamicTest> = map {
    it.asDynamicTest(executable)
}.asStream()
