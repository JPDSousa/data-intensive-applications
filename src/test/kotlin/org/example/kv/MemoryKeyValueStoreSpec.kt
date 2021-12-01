package org.example.kv

import io.kotest.common.DelicateKotest
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.Arb
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.arbitrary
import io.kotest.property.arbitrary.distinct
import io.kotest.property.arbitrary.string

@DelicateKotest
internal class MemoryKeyValueStoreSpec: ShouldSpec({
    val stringArb = Arb.string().distinct()

    include(factory = keyValueStoreTests(
        arbitrary { MemoryKeyValueStore() },
        stringArb,
        stringArb,
        PropTestConfig(maxFailure = 3, iterations = 1),
    ))
})
