package org.example.size

import java.nio.charset.Charset

object ByteArraySizeCalculator: SizeCalculator<ByteArray> {

    override fun sizeOf(value: ByteArray): Int = value.size

}

object LongSizeCalculator: SizeCalculator<Long> {

    override fun sizeOf(value: Long) = Long.SIZE_BYTES

}

class StringSizeCalculator(private val charset: Charset,
                           private val byteArraySizeCalculator: SizeCalculator<ByteArray>): SizeCalculator<String> {

    override fun sizeOf(value: String): Int = byteArraySizeCalculator.sizeOf(value.toByteArray(charset))

}
