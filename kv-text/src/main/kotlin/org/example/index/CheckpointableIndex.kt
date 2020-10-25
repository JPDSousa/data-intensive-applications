package org.example.index

import kotlinx.coroutines.*
import org.example.log.Index
import org.example.log.IndexEntry
import org.example.log.Log
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
