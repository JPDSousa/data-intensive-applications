package org.example.index

import kotlinx.coroutines.*
import org.example.log.Encoder
import org.example.log.Log
import org.example.log.LogEncoder
import org.example.log.LogFactory
import java.nio.file.Files.createFile
import java.nio.file.Path
import java.util.concurrent.Executors.newSingleThreadExecutor

internal class CheckpointableIndex<K>(private val index: Index<K>,
                                      private val checkpoint: Log<IndexEntry<K>>,
                                      private var checkpointCycle: Long = 1000L,
                                      dispatcher: CoroutineDispatcher = newSingleThreadExecutor()
                                              .asCoroutineDispatcher()): Index<K> by index {

    @Volatile
    private var volatileOps = 0L

    init {
        GlobalScope.launch(dispatcher) {
            while (true) {
                delay(1000 * 60)
                val volatileOps = this@CheckpointableIndex.volatileOps
                if (volatileOps >= checkpointCycle) {
                    checkpoint()
                    // this is possibly not thread safe
                    this@CheckpointableIndex.volatileOps -= volatileOps
                }
            }
        }
    }

    private fun checkpoint() {
        checkpoint.clear()
        checkpoint.appendAll(index.entries())
    }
}

class CheckpointableIndexFactory<K>(private val innerFactory: IndexFactory<K>,
                                    private val indexDir: Path,
                                    private val entryLogFactory: LogFactory<IndexEntry<K>>): IndexFactory<K> {

    override fun create(indexName: String): Index<K> = "index-$indexName.log"
            .let { indexDir.resolve(it) }
            .let { createFile(it) }
            .let { entryLogFactory.create(it) }
            .let { CheckpointableIndex(innerFactory.create(indexName), it) }

}

class IndexEntryLogFactory<E, K>(private val innerFactory: LogFactory<in E>,
                                 private val encoder: Encoder<IndexEntry<K>, E>): LogFactory<IndexEntry<K>> {

    override fun create(logPath: Path): Log<IndexEntry<K>> = innerFactory.create(logPath)
            .let { LogEncoder(it, encoder) }

}
