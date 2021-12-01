package org.example

import io.kotest.common.DelicateKotest
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.serialization.ExperimentalSerializationApi
import org.example.encoder.encoderModule
import org.example.generator.primitiveGeneratorsModule
import org.example.index.indexModule
import org.example.kv.kvModule
import org.example.log.logModule
import org.example.size.calculatorsModule
import org.koin.core.qualifier.Qualifier
import org.koin.core.scope.Scope
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

@DelicateKotest
@ExperimentalSerializationApi
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

internal inline fun <T, reified G: GenWrapper<T>> Scope.getAllGen(qualifiers: Iterable<Qualifier>) = qualifiers.map {
    get<G>(it).gen
}.reduce { acc, gen -> acc.merge(gen) }