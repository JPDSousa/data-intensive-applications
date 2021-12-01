package org.example.index

import io.kotest.common.DelicateKotest
import kotlinx.serialization.ExperimentalSerializationApi

@ExperimentalSerializationApi
@DelicateKotest
val indexModule = indexFactoriesModule +
        indexesModule +
        checkpointableIndexFactoriesModule +
        checkpointableIndexModule