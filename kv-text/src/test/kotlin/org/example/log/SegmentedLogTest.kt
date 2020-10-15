package org.example.log

import org.example.TestInstance
import org.example.kv.LogTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

internal class SegmentedLogTest: LogTest {

    private var logs : SegmentedLogs? = null

    @BeforeEach fun createDir() {
        this.logs = SegmentedLogs()
    }

    override fun instances() = logs!!.instances { it }.map { TestInstance(it.name, it.instance as Log) }

    @AfterEach
    fun deleteFiles() {
        logs!!.close()
    }

}
