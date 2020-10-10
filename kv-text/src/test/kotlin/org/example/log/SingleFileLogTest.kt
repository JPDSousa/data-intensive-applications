package org.example.log

import org.example.kv.LogTest
import org.example.kv.PathParameterResolverExtension
import org.junit.jupiter.api.extension.ExtendWith
import java.nio.file.Path

@ExtendWith(SingleFileLogExtension::class)
internal class SingleFileLogTest: LogTest

class SingleFileLogExtension: PathParameterResolverExtension<Log>(Log::class.java) {

    override fun createParameter(path: Path): Log = SingleFileLog(path)
}
