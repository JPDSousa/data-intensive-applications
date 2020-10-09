package org.example.log

import org.example.kv.LogTest
import org.example.kv.PathBasedExtension
import org.junit.jupiter.api.extension.ExtendWith
import java.nio.file.Path

/**
 *
 *
 * @author Joao Sousa (joao.sousa@feedzai.com)
 * @since @@@feedzai.next.release@@@
 */
@ExtendWith(SingleFileLogExtension::class)
internal class SingleFileLogTest: LogTest

class SingleFileLogExtension: PathBasedExtension<Log>(Log::class.java) {

    override fun createParameter(path: Path): Log = SingleFileLog(path)
}