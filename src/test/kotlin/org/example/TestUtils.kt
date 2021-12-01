package org.example

import io.kotest.common.DelicateKotest
import io.kotest.core.TestConfiguration
import io.kotest.property.*
import io.kotest.property.arbitrary.map
import io.kotest.property.arbitrary.merge
import kotlinx.serialization.ExperimentalSerializationApi

val defaultPropTestConfig = PropTestConfig(maxFailure = 3, iterations = 200)

fun PropTestConfig.randomSource() = seed?.random() ?: RandomSource.default()

internal fun <T> Gen<T>.merge(other: Gen<T>) = toArb().merge(other)

internal fun <T, B> Gen<T>.map(action: (T) -> B) = toArb().map(action)

private fun <T> Gen<T>.toArb(): Arb<T> = when (this) {
    is Arb -> this
    is Exhaustive -> this.toArb()
}

@ExperimentalSerializationApi
@DelicateKotest
fun TestConfiguration.bootstrapApplication() = application()
    .also {
        autoClose(object: AutoCloseable {
            override fun close() {
                it.close()
            }
        })
    }
