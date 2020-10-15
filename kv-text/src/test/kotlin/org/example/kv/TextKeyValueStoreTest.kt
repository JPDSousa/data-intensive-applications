package org.example.kv

import org.example.TestInstance
import org.example.log.TextLogs
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

class TextKeyValueStoreTest: KeyValueStoreTest {

    private var logs : TextLogs? = null

    @BeforeEach fun createFiles() {
        logs = TextLogs()
    }

    override fun instances() = logs!!.instances { TextKeyValueStore.kvLine.split(it, 2)[0] }
            .map {
        TestInstance("Text KV - ${it.name}", TextKeyValueStore(it.instance) as KeyValueStore)
    }

    @AfterEach fun deleteFiles() {
        logs!!.close()
    }
}

