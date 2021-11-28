package org.example.log

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.property.Arb
import io.kotest.property.arbitrary.byte
import io.kotest.property.arbitrary.byteArray
import io.kotest.property.arbitrary.int
import org.example.bootstrapApplication

internal class BinaryLogSpec: ShouldSpec({

    val application = bootstrapApplication()
    val generator: BinaryLogs = application.koin.get(binaryLogQ)

    include(logTests(
        generator.toArb(),
        Arb.byteArray(Arb.int(0..100), Arb.byte())
    ))
})

internal class BinaryLogFactorySpec: ShouldSpec({

    val application = bootstrapApplication()
    val logFactories: ByteArrayLogFactories = application.koin.get(binaryLogQ)

    include(logFactoryTests(
        logFactories.toArb(),
        Arb.byteArray(Arb.int(0..100), Arb.byte())
    ))
})
