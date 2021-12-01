package org.example.kv

import io.kotest.common.DelicateKotest
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.Arb
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.*
import kotlinx.serialization.ExperimentalSerializationApi
import org.example.bootstrapApplication
import org.example.possiblyArrayEquals

@DelicateKotest
@ExperimentalSerializationApi
internal class StringIndexedKeyValueStoreSpec: ShouldSpec({

    val application = bootstrapApplication()
    val kvs: StringStringKeyValueStores = application.koin.get()

    include(keyValueStoreTests(
        kvs.gen,
        Arb.string(),
        Arb.string()
            // TODO fix the harcoded filter. This should be specific to the TestInstance we're using
            .filterNot { it == Tombstone.string },
        PropTestConfig(maxFailure = 3, iterations = 300),
    ))
})

@DelicateKotest
@ExperimentalSerializationApi
internal class BinaryIndexedKeyValueStoreSpec: ShouldSpec({

    val application = bootstrapApplication()
    val kvs: LongByteArrayKeyValueStores = application.koin.get()

    include(keyValueStoreTests(
        kvs.gen,
        Arb.long(),
        Arb.byteArray(Arb.int(0..100), Arb.byte())
            // TODO fix the harcoded filter. This should be specific to the TestInstance we're using
            .filterNot { possiblyArrayEquals(it, Tombstone.byte) },
        PropTestConfig(maxFailure = 3, iterations = 300),
    ))
})
