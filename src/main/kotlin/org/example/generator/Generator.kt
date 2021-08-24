package org.example.generator

interface Generator<T> {

    fun generate(): Sequence<T>

}

class CompositeGenerator<T>(private val generators: Iterable<Generator<T>>): Generator<T> {

    override fun generate(): Sequence<T> = generators.asSequence()
        .flatMap { it.generate() }
}