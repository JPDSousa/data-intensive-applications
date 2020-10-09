package org.example.log

import org.example.kv.LogTest
import org.example.kv.PathBasedExtension
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.extension.ExtendWith
import java.nio.file.Path

/**
 *
 *
 * @author Joao Sousa (joao.sousa@feedzai.com)
 * @since @@@feedzai.next.release@@@
 */
@ExtendWith(SegmentedFileLogExtension::class)
internal class SegmentedLogTest: LogTest

class SegmentedFileLogExtension: PathBasedExtension<Log>(Log::class.java, false) {

    override fun createParameter(path: Path): Log = SegmentedLog(path)
}
