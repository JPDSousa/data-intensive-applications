package org.example

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.example.encoder.encoderModule
import org.example.generator.CompositeGenerator
import org.example.generator.primitiveGeneratorsModule
import org.example.index.indexModule
import org.example.kv.kvModule
import org.example.log.logModule
import org.example.size.calculatorsModule
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.koin.core.KoinApplication
import org.koin.core.module.Module
import org.koin.core.qualifier.Qualifier
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

inline fun <reified T, reified G: TestGenerator<T>> Module.mergeGenerators(
    qualifiers: Iterable<Qualifier>,
    crossinline block: (TestGenerator<T>) -> G) {

    single {
        block(
            TestGeneratorAdapter(
                CompositeGenerator(
                    qualifiers.map { get<G>(it) }
                )
            )
        )
    }
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

internal abstract class ApplicationTest {

    companion object Application {

        @JvmStatic
        lateinit var application: KoinApplication

        @JvmStatic
        @BeforeAll
        fun createApplication() {
            application = application()
        }

        @JvmStatic
        @AfterAll
        fun closeResources() {
            application.close()
        }

    }
}
