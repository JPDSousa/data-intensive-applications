package org.example.index

import io.kotest.property.Gen
import io.kotest.property.exhaustive.exhaustive
import org.example.GenWrapper
import org.example.getAllGen
import org.koin.dsl.module

val indexFactoriesModule = module {

    single { StringIndexFactories(
        getAllGen<IndexFactory<String>, StringIndexFactories>(indicesQ)
    ) }

    single { LongIndexFactories(
        getAllGen<IndexFactory<Long>, LongIndexFactories>(indicesQ)
    ) }

    single(hashIndexQ) { StringIndexFactories(
            listOf(HashIndexFactory<String>()).exhaustive()
    ) }

    single(hashIndexQ) { LongIndexFactories(
        listOf(HashIndexFactory<Long>()).exhaustive()
    ) }

    single(treeIndexQ) { StringIndexFactories(
            listOf(TreeIndexFactory<String>()).exhaustive()
    ) }

    single(treeIndexQ) { LongIndexFactories(
            listOf(TreeIndexFactory<Long>()).exhaustive()
    ) }
}

data class StringIndexFactories(
    override val gen: Gen<IndexFactory<String>>
) : GenWrapper<IndexFactory<String>>
data class LongIndexFactories(
    override val gen: Gen<IndexFactory<Long>>
) : GenWrapper<IndexFactory<Long>>
