package org.example

import org.example.generator.Generator

interface TestGenerator<T>: Generator<TestInstance<T>>

class TestGeneratorAdapter<T>(private val delegate: Generator<TestInstance<T>>)
    : TestGenerator<T>, Generator<TestInstance<T>> by delegate