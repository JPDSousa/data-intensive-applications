package org.example.generator

interface Generator<T>: Iterable<T> {

    fun generate(): Sequence<T>

    override fun iterator(): Iterator<T> = generate().iterator()
}

class CompositeGenerator<T>(private val generators: Iterable<Generator<T>>): Generator<T> {

    override fun generate(): Sequence<T> = generators.asSequence()
        .flatMap { it.generate() }
}