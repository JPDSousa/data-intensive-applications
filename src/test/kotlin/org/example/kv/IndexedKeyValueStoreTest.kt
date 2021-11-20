package org.example.kv

import io.kotest.common.DelicateKotest
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.Arb
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.*
import org.example.bootstrapApplication
import org.example.possiblyArrayEquals

@DelicateKotest
internal class StringIndexedKeyValueStoreSpec: ShouldSpec({

    val application = bootstrapApplication()
    val kvs: StringStringKeyValueStores = application.koin.get()

    include(keyValueStoreTests(
        kvs.toArb(),
        Arb.string(),
        Arb.string()
            // TODO fix the harcoded filter. This should be specific to the TestInstance we're using
            .filterNot { it == Tombstone.string },
        PropTestConfig(maxFailure = 3, iterations = 300),
    ))
})

@DelicateKotest
internal class BinaryIndexedKeyValueStoreSpec: ShouldSpec({

    val application = bootstrapApplication()
    val kvs: ByteArrayKeyValueStores = application.koin.get()

    include(keyValueStoreTests(
        kvs.toArb(),
        Arb.long(),
        Arb.byteArray(Arb.int(0..100), Arb.byte())
            // TODO fix the harcoded filter. This should be specific to the TestInstance we're using
            .filterNot { possiblyArrayEquals(it, Tombstone.byte) },
        PropTestConfig(maxFailure = 3, iterations = 300),
    ))
})
