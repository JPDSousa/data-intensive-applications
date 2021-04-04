package org.example.index

import kotlinx.coroutines.CoroutineDispatcher
import mu.KotlinLogging
import org.example.encoder.Encoder
import org.example.log.Log
import org.example.log.LogEncoderFactory
import org.example.log.LogFactory
import org.example.recurrent.OpsBasedRecurrentJob
import org.example.recurrent.RecurrentJob
import java.nio.file.Files.createFile
import java.nio.file.Files.notExists
import java.nio.file.Path

interface CheckpointableIndex<K>: Index<K> {

    fun checkpoint()
}

private class RecurrentCheckpointableIndex<K>(private val index: CheckpointableIndex<K>,
                                              private val checkpointJob: RecurrentJob): CheckpointableIndex<K> {

    override fun putOffset(key: K, offset: Long) {
        index.putOffset(key, offset)
        checkpointJob.registerOperation()
    }

    override fun getOffset(key: K): Long? = index.getOffset(key)
        .also { checkpointJob.registerOperation() }

    override fun entries(): Sequence<IndexEntry<K>> = index.entries()

    override fun checkpoint() {
        index.checkpoint()
    }
}

private class LogCheckpointableIndex<K>(private val index: Index<K>,
                                        private val checkpoint: Log<IndexEntry<K>>)
    : CheckpointableIndex<K>, Index<K> by index {

    override fun checkpoint() {
        checkpoint.clear()
        checkpoint.appendAll(index.entries())
    }
}

class CheckpointableIndexFactory<K>(private val innerFactory: IndexFactory<K>,
                                    private val indexDir: Path,
                                    private val entryLogFactory: LogFactory<IndexEntry<K>>,
                                    private val checkpointCycle: Long,
                                    private val coroutineDispatcher: CoroutineDispatcher): IndexFactory<K> {

    private val logger = KotlinLogging.logger {}

    override fun create(indexName: String): CheckpointableIndex<K> {

        val indexPath = "index-$indexName.log"
            .let { indexDir.resolve(it) }

        val innerIndex = innerFactory.create(indexName)

        val indexLog: Log<IndexEntry<K>>
        if (notExists(indexPath)) {
            logger.debug { "Creating index file." }
            createFile(indexPath)
            indexLog = entryLogFactory.create(indexPath)
        } else {
            logger.debug { "Index file already exists. Recovering index data." }
            indexLog = entryLogFactory.create(indexPath)
            indexLog.useEntries { innerIndex.putAllOffsets(it.asIterable()) }
        }

        return LogCheckpointableIndex(innerIndex, indexLog)
            .let { RecurrentCheckpointableIndex(it, createRecurringCheckpoint(it)) }
    }

    private fun createRecurringCheckpoint(index: CheckpointableIndex<K>): RecurrentJob {

        return OpsBasedRecurrentJob(index::checkpoint, checkpointCycle, coroutineDispatcher)
    }

}

class IndexEntryLogFactory<E, K>(innerFactory: LogFactory<E>,
                                 encoder: Encoder<IndexEntry<K>, E>): LogFactory<IndexEntry<K>> {

    private val encodedFactory = LogEncoderFactory(innerFactory, encoder)

    override fun create(logPath: Path): Log<IndexEntry<K>> = encodedFactory.create(logPath)

}
