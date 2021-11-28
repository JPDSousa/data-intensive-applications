package org.example.index

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.Arb
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.string

internal class HashIndexSpec: ShouldSpec({

    include(indexTests(
        Indexes().hashIndexes<String>().toArb(),
        Arb.string()
    ))

    include(indexTests(
        Indexes().hashIndexes<Long>().toArb(),
        Arb.long()
    ))
})
