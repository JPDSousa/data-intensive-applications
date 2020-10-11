package org.example.log

import org.example.kv.LogTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

internal class TextLogTest: LogTest {

    private var logs : TextLogs? = null

    @BeforeEach
    fun createFiles() {
        logs = TextLogs()
    }

    override fun instances() = logs!!.instances()

    @AfterEach fun deleteFiles() {
        logs!!.close()
    }

}
