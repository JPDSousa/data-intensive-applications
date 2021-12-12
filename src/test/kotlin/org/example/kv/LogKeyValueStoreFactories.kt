package org.example.kv

import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.arbitrary.bind
import org.example.GenWrapper
import org.example.generator.StringGenerator
import org.example.index.CheckpointableIndexFactory
import org.example.index.LongCheckpointableIndexFactories
import org.example.index.StringCheckpointableIndexFactories
import org.example.log.EntryLogFactory
import org.example.log.LongByteArrayMapEntryLogFactories
import org.example.log.StringStringMapEntryLogFactories
import org.example.map
import org.example.merge
import org.koin.core.scope.Scope
import org.koin.dsl.module

val logKeyValueStoreFactoryModule = module {

    single { StringStringLogKeyValueStoreFactories(
        composite<String, String, StringStringMapEntryLogFactories, StringCheckpointableIndexFactories>(Tombstone.string)
    ) }

    single { LongByteArrayLogKeyValueStoreFactories(
        composite<Long, ByteArray, LongByteArrayMapEntryLogFactories, LongCheckpointableIndexFactories>(Tombstone.byte)
    ) }

}

private inline fun <K, V, reified L: GenWrapper<EntryLogFactory<K, V>>,
        reified C: GenWrapper<CheckpointableIndexFactory<K>>> Scope.composite(tombstone: V)
        : Gen<LogKeyValueStoreFactory<K, V>> {

    val singleFactories = get<L>().gen.map {
        SingleLogKeyValueStoreFactory(it, tombstone)
    }

    val stringGenerator = get<StringGenerator>()
    return singleFactories.merge(
        Arb.bind(
            get<C>().gen,
            singleFactories,
            get<L>().gen
        ) { indexFactory, kvFactory, logFactory ->
            IndexedKeyValueStoreFactory(
                indexFactory,
                tombstone,
                kvFactory,
                logFactory,
                stringGenerator
            )
        }
    )
}

data class StringStringLogKeyValueStoreFactories(
    override val gen: Gen<LogKeyValueStoreFactory<String, String>>
) : GenWrapper<LogKeyValueStoreFactory<String, String>>
data class LongByteArrayLogKeyValueStoreFactories(
    override val gen: Gen<LogKeyValueStoreFactory<Long, ByteArray>>
) : GenWrapper<LogKeyValueStoreFactory<Long, ByteArray>>
