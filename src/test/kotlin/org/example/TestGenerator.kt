package org.example

import io.kotest.property.Arb
import io.kotest.property.arbitrary.arbitrary
import org.example.generator.Generator

interface TestGenerator<T>: Generator<TestInstance<T>> {

    fun toArb(): Arb<TestInstance<T>> {
        var seq = generate().iterator()

        return arbitrary {
            when(seq.hasNext()) {
                true -> seq.next()
                false -> {
                    seq = generate().iterator()
                    when (seq.hasNext()) {
                        true -> seq.next()
                        false -> throw IllegalStateException("Initial generator is empty")
                    }
                }
            }
        }
    }
}

class TestGeneratorAdapter<T>(private val delegate: Generator<TestInstance<T>>)
    : TestGenerator<T>, Generator<TestInstance<T>> by delegate