package org.example.log

import org.example.kv.LogTest
import org.example.kv.PathParameterResolverExtension
import org.junit.jupiter.api.extension.ExtendWith
import java.nio.file.Path


@ExtendWith(SegmentedFileLogExtension::class)
internal class SegmentedLogTest: LogTest

class SegmentedFileLogExtension: PathParameterResolverExtension<Log>(Log::class.java, false) {

    override fun createParameter(path: Path): Log = SegmentedLog(path, 5)
}
