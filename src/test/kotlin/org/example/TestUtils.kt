package org.example

import io.kotest.core.TestConfiguration
import io.kotest.property.PropTestConfig
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.example.encoder.encoderModule
import org.example.generator.CompositeGenerator
import org.example.generator.primitiveGeneratorsModule
import org.example.index.indexModule
import org.example.kv.kvModule
import org.example.log.logModule
import org.example.size.calculatorsModule
import org.koin.core.module.Module
import org.koin.core.qualifier.Qualifier
import org.koin.dsl.koinApplication
import org.koin.dsl.module
import java.time.Clock
import java.util.concurrent.Executors

private val testModule = module {
    single { Charsets.UTF_8 }
    single { TestResources() }
    single<CoroutineDispatcher> { Executors.newSingleThreadExecutor().asCoroutineDispatcher() }
    single { Clock.systemDefaultZone() }
}

val defaultPropTestConfig = PropTestConfig(maxFailure = 3, iterations = 100)

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

fun TestConfiguration.bootstrapApplication() = application()
    .also {
        autoClose(object: AutoCloseable {
            override fun close() {
                it.close()
            }
        })
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

