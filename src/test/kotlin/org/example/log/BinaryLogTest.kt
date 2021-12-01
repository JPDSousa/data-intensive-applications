package org.example.log

import io.kotest.common.DelicateKotest
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.Arb
import io.kotest.property.arbitrary.byte
import io.kotest.property.arbitrary.byteArray
import io.kotest.property.arbitrary.int
import kotlinx.serialization.ExperimentalSerializationApi
import org.example.bootstrapApplication

@DelicateKotest
@ExperimentalSerializationApi
internal class BinaryLogSpec: ShouldSpec({

    val application = bootstrapApplication()
    val generator = application.koin.get<BinaryLogs>(binaryLogQ)

    include(logTests(
        generator.gen,
        Arb.byteArray(Arb.int(0..100), Arb.byte())
    ))
})

@DelicateKotest
@ExperimentalSerializationApi
internal class BinaryLogFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val logFactories: ByteArrayLogFactories = application.koin.get(binaryLogQ)

    include(logFactoryTests(
        logFactories.gen,
        Arb.byteArray(Arb.int(0..100), Arb.byte())
    ))
})
