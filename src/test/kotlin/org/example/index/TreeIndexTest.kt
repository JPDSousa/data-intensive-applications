package org.example.index

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.Arb
import io.kotest.property.arbitrary.string

internal class TreeIndexSpec: ShouldSpec({

    include(indexTests(
        Indexes().treeIndexes<String>().toArb(),
        Arb.string()
    ))
})

