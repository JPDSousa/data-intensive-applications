package org.example.index

import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.arbitrary.bind
import kotlinx.coroutines.CoroutineDispatcher
import org.example.GenWrapper
import org.example.TestResources
import org.example.encoder.Encoder
import org.example.encoder.Instant2ByteArrayEncoders
import org.example.encoder.Instant2StringEncoders
import org.example.log.ByteArrayLogFactories
import org.example.log.LogFactoryB
import org.example.log.StringLogFactories
import org.example.merge
import org.koin.dsl.module
import java.time.Clock
import java.time.Instant

val checkpointableIndexFactoriesModule = module {

    single { StringCheckpointableIndexFactories(checkpointableIndexFactories(
        get(),
        get(),
        get<StringIndexCheckpointStoreFactories>().gen,
        get<StringIndexFactories>().gen
    )) }

    single { LongCheckpointableIndexFactories(checkpointableIndexFactories(
                get(),
                get(),
                get<LongIndexCheckpointStoreFactories>().gen,
                get<LongIndexFactories>().gen
    )) }

    single {

        val stringGen = indexCheckpointStoreFactories<String, String>(
            get(),
            get<StringLogFactories>().gen,
            get<Instant2StringEncoders>().gen,
            get<StringIndexEntryLogFactories>().gen,
        )

        val byteArrayGen = indexCheckpointStoreFactories<ByteArray, String>(
            get(),
            get<ByteArrayLogFactories>().gen,
            get<Instant2ByteArrayEncoders>().gen,
            get<StringIndexEntryLogFactories>().gen,
        )

        return@single StringIndexCheckpointStoreFactories(stringGen.merge(byteArrayGen))
    }

    single {

        val stringGen = indexCheckpointStoreFactories<String, Long>(
            get(),
            get<StringLogFactories>().gen,
            get<Instant2StringEncoders>().gen,
            get<LongIndexEntryLogFactories>().gen,
        )

        val byteArrayGen = indexCheckpointStoreFactories<ByteArray, Long>(
            get(),
            get<ByteArrayLogFactories>().gen,
            get<Instant2ByteArrayEncoders>().gen,
            get<LongIndexEntryLogFactories>().gen,
        )

        return@single LongIndexCheckpointStoreFactories(stringGen.merge(byteArrayGen))
    }
}

data class StringCheckpointableIndexFactories(
    override val gen: Gen<CheckpointableIndexFactory<String>>
) : GenWrapper<CheckpointableIndexFactory<String>>
data class LongCheckpointableIndexFactories(
    override val gen: Gen<CheckpointableIndexFactory<Long>>
) : GenWrapper<CheckpointableIndexFactory<Long>>

data class StringIndexCheckpointStoreFactories(
    override val gen: Gen<IndexCheckpointStoreFactory<String>>
) : GenWrapper<IndexCheckpointStoreFactory<String>>
data class LongIndexCheckpointStoreFactories(
    override val gen: Gen<IndexCheckpointStoreFactory<Long>>
) : GenWrapper<IndexCheckpointStoreFactory<Long>>

private fun <S, K> indexCheckpointStoreFactories(
    clock: Clock,
    metadataFactories: Gen<LogFactoryB<S>>,
    instantEncoders: Gen<Encoder<Instant, S>>,
    entryLogFactories: Gen<IndexLogFactory<K>>,
): Gen<IndexCheckpointStoreFactory<K>> = Arb.bind(
    metadataFactories,
    instantEncoders,
    entryLogFactories
) { metadataFactory, instantEncoder, entryLogFactory ->
    LogBackedIndexCheckpointStoreFactory(
        clock,
        metadataFactory,
        instantEncoder,
        entryLogFactory
    )
}

private fun <K> checkpointableIndexFactories(
    resources: TestResources,
    dispatcher: CoroutineDispatcher,
    indexCheckpointStoreFactories: Gen<IndexCheckpointStoreFactory<K>>,
    indexFactories: Gen<IndexFactory<K>>
): Gen<CheckpointableIndexFactory<K>> = Arb.bind(
    indexCheckpointStoreFactories,
    indexFactories
) { checkpointStoreFactory, indexFactory ->

    CheckpointableIndexFactory(
        checkpointStoreFactory,
        indexFactory,
        resources.allocateTempDir("index-string-"),
        10_000,
        dispatcher
    )
}
