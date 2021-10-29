package org.example.index

import kotlinx.coroutines.CoroutineDispatcher
import mu.KotlinLogging
import org.example.concepts.Factory
import org.example.encoder.Encoder
import org.example.log.Log
import org.example.log.LogEncoderFactory
import org.example.log.LogFactory
import org.example.recurrent.OpsBasedRecurrentJob
import org.example.recurrent.RecurrentJob
import org.example.size.CheckpointStore
import java.nio.file.Files.createFile
import java.nio.file.Files.notExists
import java.nio.file.Path
import java.time.Clock
import java.time.Instant

interface CheckpointableIndex<K>: Index<K> {

    fun checkpoint()

    val lastCheckpoint: Instant?

    val lastOffset: Long
        get() = 0L
}

/**
 * Responsible for checkpointing an index.
 *
 * Uses two separate structures:
 * - [sink] to persist the index content
 * - [metadata] to persist metadata related with the checkpointing operation (checkpoint instant).
 */
internal class IndexCheckpointStore<K, M>(private val clock: Clock,
                                          private val metadata: Log<M>,
                                          private val instantEncoder: Encoder<Instant, M>,
                                          private val sink: Log<IndexEntry<K>>,
                                          override var lastInstant: Instant?
) : CheckpointStore<Sequence<IndexEntry<K>>> {

    private val logger = KotlinLogging.logger {}

    override fun checkpointState(state: Sequence<IndexEntry<K>>) {

        sink.clear()
        metadata.clear()

        clock.instant()
            .also { this.lastInstant = it }
            .let { instantEncoder.encode(it) }
            .let { metadata.append(it) }

        val nEntries = sink.appendAll(state).count()

        logger.trace { "Checkpointed $nEntries" }
    }

    override fun <R> useLastState(block: (Sequence<IndexEntry<K>>?) -> R) = sink.useEntries {
        block(it)
    }

}

@Factory(IndexCheckpointStore::class)
class LogBackedIndexCheckpointStoreFactory<K, M>(private val clock: Clock,
                                                 private val metadataLogFactory: LogFactory<M>,
                                                 private val instantEncoder: Encoder<Instant, M>,
                                                 private val entryLogFactory: LogFactory<IndexEntry<K>>)
    : IndexCheckpointStoreFactory<K> {

    private val logger = KotlinLogging.logger {}

    override fun createCheckpointStore(storePath: Path): CheckpointStore<Sequence<IndexEntry<K>>> {
        if(notExists(storePath)) {
            logger.debug { "Creating index file." }
            createFile(storePath)
        }

        val metadata = loadMetadataLog(storePath)
        val lastInstant = metadata.useEntries {
            it.firstOrNull()?.let { millis -> instantEncoder.decode(millis) }
        }
        return IndexCheckpointStore(
            clock,
            metadata,
            instantEncoder,
            entryLogFactory.create(storePath),
            lastInstant
        )
    }

    private fun loadMetadataLog(indexPath: Path): Log<M> {
        val metadataPath = indexPath.parent
            .resolve("${indexPath.fileName}_meta")

        if (notExists(metadataPath)) {
            logger.debug { "Creating index metadata file." }
            createFile(metadataPath)
        } else {
            logger.debug { "Index metadata file already exists. Recovering metadata." }
        }

        return metadataLogFactory.create(metadataPath)
    }
}


interface IndexCheckpointStoreFactory<K> {

    fun createCheckpointStore(storePath: Path): CheckpointStore<Sequence<IndexEntry<K>>>

}

/**
 * Responsible for running a [CheckpointableIndex.checkpoint] operation recurrently, via [checkpointJob].
 */
private class RecurrentCheckpointableIndex<K>(private val index: CheckpointableIndex<K>,
                                              private val checkpointJob: RecurrentJob): CheckpointableIndex<K> by index {

    override fun put(key: K, value: Long) {
        index[key] = value
        checkpointJob.registerOperation()
    }

}

/**
 * Class that elementary implements operation of [CheckpointableIndex.checkpoint].
 */
private class SingleCheckpointableIndex<K>(private val index: Index<K>,
                                           private val store: CheckpointStore<Sequence<IndexEntry<K>>>)
    : CheckpointableIndex<K>, Index<K> by index {

    override fun checkpoint() {
        index.useEntries {
            store.checkpointState(it)
        }
    }

    override val lastCheckpoint: Instant?
        get() = store.lastInstant
}

class CheckpointableIndexFactory<K>(private val storeFactory: IndexCheckpointStoreFactory<K>,
                                       private val innerFactory: IndexFactory<K>,
                                       private val indexDir: Path,
                                       private val checkpointCycle: Long,
                                       private val coroutineDispatcher: CoroutineDispatcher): IndexFactory<K> {

    override fun create(indexName: String): CheckpointableIndex<K> {

        val innerIndex = innerFactory.create(indexName)

        return resolveIndexPath(indexName)
            .let { storeFactory.createCheckpointStore(it) }
            .also { innerIndex.loadLastCheckpoint(it) }
                // create base checkpointable index
            .let { indexStore -> SingleCheckpointableIndex(innerIndex, indexStore) }
                // wrap around recurrent job
            .let { RecurrentCheckpointableIndex(it, createRecurringCheckpoint(it)) }
    }

    private fun createRecurringCheckpoint(index: CheckpointableIndex<K>): RecurrentJob {

        return OpsBasedRecurrentJob(index::checkpoint, checkpointCycle, coroutineDispatcher)
    }

    private fun resolveIndexPath(indexName: String) = indexDir.resolve("index-$indexName.log")

}

private fun <K> Index<K>.loadLastCheckpoint(store: CheckpointStore<Sequence<IndexEntry<K>>>) {
    store.useLastState {
        if (it != null) {
            putAllOffsets(it.asIterable())
        }
    }
}

class IndexEntryLogFactory<E, K>(innerFactory: LogFactory<E>,
                                 encoder: Encoder<IndexEntry<K>, E>): LogFactory<IndexEntry<K>> {

    private val encodedFactory = LogEncoderFactory(innerFactory, encoder)

    override fun create(logPath: Path): Log<IndexEntry<K>> = encodedFactory.create(logPath)

}
