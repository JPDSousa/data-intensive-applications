package org.example.generator

import org.koin.dsl.module
import java.util.concurrent.atomic.AtomicLong

val primitiveGeneratorsModule = module {

    single<ByteArrayGenerator> { ByteArrayGeneratorImpl(get()) }
    single<StringGenerator> { StringGeneratorImpl(get()) }
    single<LongGenerator> { LongGeneratorImpl() }
}


private class ByteArrayGeneratorImpl(private val stringGenerator: StringGenerator): ByteArrayGenerator {
    override fun generate() = stringGenerator.generate()
        .map { it.toByteArray() }
}


private class StringGeneratorImpl(private val longGenerator: LongGenerator): StringGenerator {
    override fun generate() = longGenerator.generate()
        .map { it.toString() }
}


private class LongGeneratorImpl(private val atomicLong: AtomicLong = AtomicLong()): LongGenerator {
    override fun generate(): Sequence<Long> = generateSequence { atomicLong.getAndIncrement() }
}
interface ByteArrayGenerator: Generator<ByteArray>
interface StringGenerator: Generator<String>

interface LongGenerator: Generator<Long>
