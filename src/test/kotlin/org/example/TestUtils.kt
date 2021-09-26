package org.example

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.example.encoder.encodersModule
import org.example.generator.primitiveGeneratorsModule
import org.example.index.checkpointableIndexFactoriesModule
import org.example.index.checkpointableIndexModule
import org.example.index.indexFactoriesModule
import org.example.index.indexesModule
import org.example.kv.keyValueStoresModule
import org.example.kv.logKeyValueStoreFactoryModule
import org.example.kv.lsm.lsmKeyValueStoreFactoriesModule
import org.example.kv.lsm.sequential.segmentManagersModule
import org.example.kv.lsm.sequentialSegmentManagersModule
import org.example.kv.lsm.sstable.sstableSegmentManagersModule
import org.example.log.logFactoriesModule
import org.example.log.logsModule
import org.example.size.calculatorsModule
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.koin.dsl.koinApplication
import org.koin.dsl.module
import java.nio.charset.Charset
import java.time.Clock
import java.util.concurrent.Executors

fun assertPossiblyArrayEquals(expected: Any?, actual: Any?) {

    if (expected is ByteArray && actual is ByteArray) {
        assertArrayEquals(expected, actual)
    } else {
        assertEquals(expected, actual)
    }
}

private val testModule = module {
    single { Charsets.UTF_8 }
    single { TestResources() }
    single<CoroutineDispatcher> { Executors.newSingleThreadExecutor().asCoroutineDispatcher() }
    single { Clock.systemDefaultZone() }
}

fun application() = koinApplication {
    modules(
        primitiveGeneratorsModule,
        calculatorsModule,
        testModule,
        encodersModule,
        logsModule,
        logFactoriesModule,
        indexesModule,
        indexFactoriesModule,
        checkpointableIndexModule,
        checkpointableIndexFactoriesModule,
        sequentialSegmentManagersModule,
        segmentManagersModule,
        sstableSegmentManagersModule,
        keyValueStoresModule,
        logKeyValueStoreFactoryModule,
        lsmKeyValueStoreFactoriesModule
    )
}
