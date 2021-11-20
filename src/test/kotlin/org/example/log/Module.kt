package org.example.log

val delegateQualifiers = listOf(logEncoderQ, sizeLogQ)
val stringQualifiers = listOf(lineLogQ) + delegateQualifiers
val byteArrayQualifiers = listOf(binaryLogQ) + delegateQualifiers

val logModule = logFactoriesModule + logsModule