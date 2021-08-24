package org.example.index

import kotlinx.coroutines.CoroutineDispatcher
import org.example.TestGenerator
import org.example.TestInstance
import org.example.TestResources
import org.example.encoder.Encoder
import org.example.encoder.Encoders
import org.example.encoder.Instant2ByteArrayEncoders
import org.example.encoder.Instant2StringEncoders
import org.example.generator.Generator
import org.example.log.ByteArrayLogFactories
import org.example.log.LogFactories
import org.example.log.LogFactory
import org.example.log.StringLogFactories
import org.koin.dsl.module
import java.time.Clock
import java.time.Instant

val checkpointableIndexFactoriesModule = module {

    single<StringCheckpointableIndexFactories> {
        val stringString: FactoriesConfig<String, String> = FactoriesConfig(
            get<StringLogFactories>(),
            get<Instant2StringEncoders>(),
            get<StringIndexEntryLogFactories>(),
            get<StringIndexFactories>()
        )
        val byteArrayString: FactoriesConfig<ByteArray, String> = FactoriesConfig(
            get<ByteArrayLogFactories>(),
            get<Instant2ByteArrayEncoders>(),
            get<StringIndexEntryLogFactories>(),
            get<StringIndexFactories>()
        )

        return@single DelegateStringCheckpointableIndexFactories(
            GenericCheckpointableIndexFactories(
                get(),
                get(),
                listOf(
                    stringString,
                    byteArrayString
                ),
                get()
            )
        )
    }

    single<LongCheckpointableIndexFactories> {
        val stringString: FactoriesConfig<String, Long> = FactoriesConfig(
            get<StringLogFactories>(),
            get<Instant2StringEncoders>(),
            get<LongIndexEntryLogFactories>(),
            get<LongIndexFactories>()
        )
        val byteArrayString: FactoriesConfig<ByteArray, Long> = FactoriesConfig(
            get<ByteArrayLogFactories>(),
            get<Instant2ByteArrayEncoders>(),
            get<LongIndexEntryLogFactories>(),
            get<LongIndexFactories>()
        )

        return@single DelegateLongCheckpointableIndexFactories(
            GenericCheckpointableIndexFactories(
                get(),
                get(),
                listOf(
                    stringString,
                    byteArrayString
                ),
                get()
            )
        )
    }
}

private data class FactoriesConfig<out M, K>(
    val factories: LogFactories<@UnsafeVariance M>,
    val instantEncoders: Encoders<Instant, @UnsafeVariance M>,
    val entryLogFactory: IndexEntryLogFactories<K>,
    val indexFactories: IndexFactories<K>
)

interface CheckpointableIndexFactories<K>: TestGenerator<CheckpointableIndexFactory<K>>

interface StringCheckpointableIndexFactories: CheckpointableIndexFactories<String>

interface LongCheckpointableIndexFactories: CheckpointableIndexFactories<Long>

private class DelegateStringCheckpointableIndexFactories(private val delegate: CheckpointableIndexFactories<String>)
    : StringCheckpointableIndexFactories, CheckpointableIndexFactories<String> by delegate

private class DelegateLongCheckpointableIndexFactories(private val delegate: CheckpointableIndexFactories<Long>)
    : LongCheckpointableIndexFactories, CheckpointableIndexFactories<Long> by delegate

private class GenericCheckpointableIndexFactories<K>(private val resources: TestResources,
                                                     private val dispatcher: CoroutineDispatcher,
                                                     private val configs: Iterable<FactoriesConfig<Any, K>>,
                                                     private val clock: Clock
): CheckpointableIndexFactories<K> {

    private fun createCheckpointableIndex(checkpointStoreFactory: IndexCheckpointStoreFactory<K>,
                                          indexFactory: IndexFactory<K>) = CheckpointableIndexFactory(
        checkpointStoreFactory,
        indexFactory,
        resources.allocateTempDir("index-string-"),
        10_000,
        dispatcher
    )

    private fun <S> createCheckpointStoreFactory(metadataLogFactory: LogFactory<S>,
                                                 instantSerializer: TestInstance<Encoder<Instant, S>>,
                                                 indexLogFactory: LogFactory<IndexEntry<K>>) =
        LogBackedIndexCheckpointStoreFactory(
            clock,
            metadataLogFactory,
            instantSerializer.instance(),
            indexLogFactory
        )

    override fun generate(): Sequence<TestInstance<CheckpointableIndexFactory<K>>> = sequence {

        for (config in configs) {

            for (factory in config.factories.generate()) {

                for (instantEncoder in config.instantEncoders.generate()) {

                    for (entryLogFactory in config.entryLogFactory.generate()) {

                        for (indexFactory in config.indexFactories.generate()) {

                            yield(TestInstance("Checkpointable String Index ~ ${factory.name}") {

                                createCheckpointableIndex(
                                    createCheckpointStoreFactory(
                                        factory.instance(),
                                        instantEncoder,
                                        entryLogFactory.instance()
                                    ),
                                    indexFactory.instance()
                                )
                            })
                        }
                    }
                }

            }
        }
    }
}