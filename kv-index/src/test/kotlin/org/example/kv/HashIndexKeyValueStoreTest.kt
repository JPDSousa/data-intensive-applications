package org.example.kv

import org.example.TestInstance
import org.example.log.TextLogs
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

internal class HashIndexKeyValueStoreTest: KeyValueStoreTest {

    private var logs : TextLogs? = null

    @BeforeEach
    fun createFiles() {
        logs = TextLogs()
    }

    override fun instances(): Sequence<TestInstance<KeyValueStore>> {

        return logs!!.instances { TextKeyValueStore.kvLine.split(it, 2)[0] }.map {
            TestInstance("Text KV - ${it.name}", TextKeyValueStore(it.instance) as SeekableKeyValueStore)
        }.map {
            TestInstance("Index - ${it.name}", HashIndexKeyValueStore(it.instance) as KeyValueStore)
        }
    }

    @AfterEach fun deleteFiles() {
        logs!!.close()
    }
}


