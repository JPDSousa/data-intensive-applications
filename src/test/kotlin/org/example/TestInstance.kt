package org.example

import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestInfo
import java.util.stream.Stream
import kotlin.streams.asStream

data class TestInstance<out T>(private val name: String, val instance: () -> T) {

    fun asDynamicTest(info: TestInfo, executable: (T) -> Unit): DynamicTest
    = dynamicTest("${info.displayName} :: $name") { executable(instance()) }

    override fun toString() = name
}

fun <T> Sequence<TestInstance<T>>.test(info: TestInfo, executable: (T) -> Unit): Stream<DynamicTest> = map {
    it.asDynamicTest(info, executable)
}.asStream()


