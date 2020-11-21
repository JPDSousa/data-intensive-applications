package org.example.log

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8

class LogFactory {

    fun stringEncoder(log: Log<ByteArray>, charset: Charset = UTF_8) = LogEncoder(
            log,
            { it.toByteArray(charset)},
            { String(it, charset) }
    )
}
