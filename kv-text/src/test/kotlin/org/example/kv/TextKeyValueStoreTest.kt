package org.example.kv

import org.example.TestInstance
import org.example.index.CheckpointableIndex
import org.example.log.TextLogs
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.nio.file.Files.createTempDirectory

class TextKeyValueStoreTest: KeyValueStoreTest {

    private var logs : TextLogs? = null

    @BeforeEach fun createFiles() {
        logs = TextLogs()
    }

    override fun instances() = logs!!.instances().map {

        val log = it.instance
        val indexDir = createTempDirectory("${it.name}-index-")
        val index = CheckpointableIndex(indexDir, log::size)

        TestInstance("Text KV - ${it.name}", TextKeyValueStore(index, log) as KeyValueStore)
    }

    @AfterEach fun deleteFiles() {
        logs!!.close()
    }
}

