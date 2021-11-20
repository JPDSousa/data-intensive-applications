package org.example.kv

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.Arb
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.string
import org.example.DataEntry
import org.example.bootstrapApplication
import org.example.log.StringStringMapEntryLogFactories

internal class StringIndexedKeyValueStoreFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val factories: StringStringLogKeyValueStoreFactories = application.koin.get()
    val logFactories: StringStringMapEntryLogFactories = application.koin.get()

    include(logKeyValueStoreFactoryTests(
        factories.toArb(),
        logFactories.toArb(),
        Arb.bind(Arb.string(), Arb.string()) { key, value ->
            DataEntry(key, value)
        }
    ))

})

