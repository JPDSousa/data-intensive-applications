package org.example.index

import io.kotest.common.DelicateKotest
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.Arb
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.string
import kotlinx.serialization.ExperimentalSerializationApi
import org.example.bootstrapApplication

@DelicateKotest
@ExperimentalSerializationApi
internal class HashIndexSpec: ShouldSpec({

    val application = bootstrapApplication()

    include(indexTests(
        application.koin.get<StringIndices>(hashIndexQ).gen,
        Arb.string()
    ))

    include(indexTests(
        application.koin.get<LongIndices>(hashIndexQ).gen,
        Arb.long()
    ))
})
