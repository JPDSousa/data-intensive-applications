package org.example.kv.lsm.sequential

import org.koin.core.definition.Definition
import org.koin.core.instance.InstanceFactory
import org.koin.core.module.Module
import org.koin.core.qualifier.named

val sequentialQ = named("sequentialSegmentManager")

inline fun <reified T> Module.singleSequentialQ(
    createdAtStart: Boolean = false,
    noinline definition: Definition<T>
): Pair<Module, InstanceFactory<T>> = single(sequentialQ, createdAtStart, definition)

val sequentialModule = sequentialOpenSegmentFactories + segmentMergeStrategiesModule
