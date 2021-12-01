package org.example.index

import io.kotest.property.Gen
import org.example.GenWrapper
import org.example.map
import org.koin.dsl.module

val checkpointableIndexModule = module {

    single { StringCheckpointableIndexes(indexFromFactory(
        get<StringCheckpointableIndexFactories>().gen
    )) }

    single { LongCheckpointableIndexes(indexFromFactory(
        get<LongCheckpointableIndexFactories>().gen
    )) }

}

private fun <K> indexFromFactory(factories: Gen<CheckpointableIndexFactory<K>>) = factories.map {
    it.create("${CheckpointableIndex::class.simpleName} from $it")
}

data class StringCheckpointableIndexes(
    override val gen: Gen<CheckpointableIndex<String>>
) : GenWrapper<CheckpointableIndex<String>>
data class LongCheckpointableIndexes(
    override val gen: Gen<CheckpointableIndex<Long>>
) : GenWrapper<CheckpointableIndex<Long>>
