package org.example.index

import org.example.TestGenerator
import org.example.TestGeneratorAdapter
import org.example.TestInstance
import org.example.generator.CompositeGenerator
import org.koin.dsl.module

val indexFactoriesModule = module {
    
    single<StringIndexFactories> {
        DelegateStringIndexFactories(
            TestGeneratorAdapter(
                CompositeGenerator(listOf(
                    get<StringIndexFactories>(treeIndexQ),
                    get<StringIndexFactories>(hashIndexQ)
                ))
            )
        )
    }
    
    single<StringIndexFactories>(treeIndexQ) {
        DelegateStringIndexFactories(TreeIndexFactories())
    }

    single<StringIndexFactories>(hashIndexQ) {
        DelegateStringIndexFactories(HashIndexFactories())
    }

    single<LongIndexFactories> {
        DelegateLongIndexFactories(
            TestGeneratorAdapter(
                CompositeGenerator(listOf(
                    get<LongIndexFactories>(hashIndexQ)
                ))
            )
        )
    }

    single<LongIndexFactories>(hashIndexQ) {
        DelegateLongIndexFactories(HashIndexFactories())
    }
}

interface IndexFactories<K>: TestGenerator<IndexFactory<K>>

interface ComparableIndexFactories<K: Comparable<K>>: IndexFactories<K>

interface StringIndexFactories: ComparableIndexFactories<String>

interface LongIndexFactories: IndexFactories<Long>

private class DelegateStringIndexFactories(private val delegate: TestGenerator<IndexFactory<String>>)
    : StringIndexFactories, TestGenerator<IndexFactory<String>> by delegate

private class DelegateLongIndexFactories(private val delegate: TestGenerator<IndexFactory<Long>>)
    : LongIndexFactories, TestGenerator<IndexFactory<Long>> by delegate

private class HashIndexFactories<K>: IndexFactories<K> {

    override fun generate(): Sequence<TestInstance<IndexFactory<K>>> = sequenceOf(
        TestInstance<IndexFactory<K>>("HashIndexFactory") {
            HashIndexFactory()
        }
    )
}

private class TreeIndexFactories<K: Comparable<K>>: ComparableIndexFactories<K> {

    override fun generate(): Sequence<TestInstance<IndexFactory<K>>> = sequenceOf(
        TestInstance<IndexFactory<K>>("Tree index factory") {
            TreeIndexFactory()
        }
    )
}
