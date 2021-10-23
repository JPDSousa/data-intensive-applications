package org.example

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.example.encoder.encoderModule
import org.example.generator.primitiveGeneratorsModule
import org.example.index.indexModule
import org.example.kv.kvModule
import org.example.log.logModule
import org.example.size.calculatorsModule
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.koin.dsl.koinApplication
import org.koin.dsl.module
import java.time.Clock
import java.util.concurrent.Executors

fun assertPossiblyArrayEquals(expected: Any?, actual: Any?) {

    if (expected is ByteArray && actual is ByteArray) {
        assertArrayEquals(expected, actual)
    } else {
        assertEquals(expected, actual)
    }
}

private val testModule = module {
    single { Charsets.UTF_8 }
    single { TestResources() }
    single<CoroutineDispatcher> { Executors.newSingleThreadExecutor().asCoroutineDispatcher() }
    single { Clock.systemDefaultZone() }
}

fun application() = koinApplication {

    val allModules =
        // external modules
        primitiveGeneratorsModule +
                calculatorsModule +
                // module from this file
                testModule +
                kvModule +
                logModule +
                encoderModule +
                indexModule

    modules(allModules)
}
