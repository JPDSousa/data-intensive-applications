package org.example.index

import kotlinx.coroutines.*
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.example.log.Index
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files.*
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import java.util.*
import java.util.concurrent.Executors

class CheckpointableIndex(private val checkpointDir: Path,
                          private val versionGenerator: () -> Long,
                          dispatcher: CoroutineDispatcher = Executors.newSingleThreadExecutor()
                                  .asCoroutineDispatcher(),
                          private val index: MutableMap<String, Long> = TreeMap<String, Long>(),
                          private var version: Long = 0L,
                          private var checkpointCycle: Long = 1000L,
private val charset: Charset = UTF_8): Index {

    @Volatile
    private var volatileOps = 0L

    init {
        if (notExists(this.checkpointDir)) {
            createDirectories(checkpointDir)
        }
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
        val checkpointPath = this.checkpointDir.resolve("index-checkpoint-$version")
        val content = Json.encodeToString(this.index).toByteArray(this.charset)

        write(checkpointPath, content, CREATE, TRUNCATE_EXISTING)
    }

    override fun putOffset(key: String, offset: Long) {
        index[key] = offset
        version = versionGenerator()
    }

    override fun putAllOffsets(pairs: Iterable<Pair<String, Long>>) {
        index.putAll(pairs)
        version = versionGenerator()
    }

    override fun getOffset(key: String): Long? = index[key]
}
