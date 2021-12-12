package org.example.index

import kotlinx.coroutines.CoroutineDispatcher
import mu.KotlinLogging
import org.example.concepts.Factory
import org.example.encoder.Encoder
import org.example.log.Log
import org.example.log.LogFactoryB
import org.example.recurrent.OpsBasedRecurrentJob
import org.example.recurrent.RecurrentJob
import org.example.size.CheckpointStore
import java.nio.file.Files.createFile
import java.nio.file.Files.notExists
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock

typealias IndexLogFactory<K> = LogFactoryB<IndexEntry<K>>

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
private class IndexCheckpointStore<K, M>(
    private val clock: Clock,
    private val storePath: Path,
    private val metadataLogFactory: LogFactoryB<M>,
    private val instantEncoder: Encoder<Instant, M>,
    private val sinkLogFactory: IndexLogFactory<K>,
    override var lastInstant: Instant?
) : CheckpointStore<Sequence<IndexEntry<K>>> {

    private val logger = KotlinLogging.logger {}
    private val lock = ReentrantLock()

    private var metadata = resetMetadata()
    private var sink = resetSink()

    private fun resetMetadata() = lazy(lock) {
        val metadataPath = storePath.parent
            .resolve("${storePath.fileName}_meta")

        if (notExists(metadataPath)) {
            logger.debug { "Creating index metadata file." }
            createFile(metadataPath)
        } else {
            logger.debug { "Index metadata file already exists. Recovering metadata." }
        }
        metadataLogFactory.create(metadataPath)
    }
    private fun resetSink() = lazy(lock) { sinkLogFactory.create(storePath) }

    override fun checkpointState(state: Sequence<IndexEntry<K>>) {

        if (metadata.isInitialized()) {
            metadataLogFactory.trash.mark(metadata.value)
            metadata = resetMetadata()
        }
        if (sink.isInitialized()) {
            sinkLogFactory.trash.mark(sink.value)
            sink = resetSink()
        }

        clock.instant()
            .also { this.lastInstant = it }
            .let { instantEncoder.encode(it) }
            .let { metadata.value.append(it) }

        val nEntries = sink.value.appendAll(state).count()

        logger.trace { "Checkpointed $nEntries" }
    }

    override fun <R> useLastState(block: (Sequence<IndexEntry<K>>?) -> R) = sink.value.useEntries {
        block(it)
    }

}

@Factory(IndexCheckpointStore::class)
class LogBackedIndexCheckpointStoreFactory<K, M>(
    private val clock: Clock,
    private val metadataLogFactory: LogFactoryB<M>,
    private val instantEncoder: Encoder<Instant, M>,
    private val entryLogFactory: LogFactoryB<IndexEntry<K>>
) : IndexCheckpointStoreFactory<K> {

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
            storePath,
            metadataLogFactory,
            instantEncoder,
            entryLogFactory,
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
private class RecurrentCheckpointableIndex<K>(
    private val index: CheckpointableIndex<K>,
    private val checkpointJob: RecurrentJob
): CheckpointableIndex<K> by index {

    override fun put(key: K, value: Long) {
        index[key] = value
        checkpointJob.registerOperation()
    }

}

/**
 * Class that elementary implements operation of [CheckpointableIndex.checkpoint].
 */
private class SingleCheckpointableIndex<K>(
    private val index: Index<K>,
    private val store: CheckpointStore<Sequence<IndexEntry<K>>>
) : CheckpointableIndex<K>, Index<K> by index {

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

        return OpsBasedRecurrentJob(checkpointCycle, coroutineDispatcher, index::checkpoint)
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
