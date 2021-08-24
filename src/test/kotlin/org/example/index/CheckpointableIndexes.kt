package org.example.index

import org.example.TestGenerator
import org.example.TestInstance
import org.example.generator.Generator
import org.koin.dsl.module

val checkpointableIndexModule = module {

    single<StringCheckpointableIndexes> { DelegateStringCheckpointableIndexes(
        GenericCheckpointableIndexes(get<StringCheckpointableIndexFactories>())
    )}

    single<LongCheckpointableIndexes> { DelegateLongCheckpointableIndexes(
        GenericCheckpointableIndexes(get<LongCheckpointableIndexFactories>())
    )}

}

interface CheckpointableIndexes<K>: TestGenerator<CheckpointableIndex<K>>

interface StringCheckpointableIndexes: CheckpointableIndexes<String>

interface LongCheckpointableIndexes: CheckpointableIndexes<Long>

private class DelegateStringCheckpointableIndexes(private val delegate: CheckpointableIndexes<String>)
    : StringCheckpointableIndexes, CheckpointableIndexes<String> by delegate

private class DelegateLongCheckpointableIndexes(private val delegate: CheckpointableIndexes<Long>)
    : LongCheckpointableIndexes, CheckpointableIndexes<Long> by delegate

private class GenericCheckpointableIndexes<K>(private val indexFactories: CheckpointableIndexFactories<K>)
    : CheckpointableIndexes<K> {

    override fun generate(): Sequence<TestInstance<CheckpointableIndex<K>>> = indexFactories.generate().map {
        TestInstance("Checkpointable index from ${it.name}") {
            it.instance().create("Checkpointable index from ${it.name}")
        }
    }

}
