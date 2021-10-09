package org.example.kv.lsm.sstable

import org.koin.core.definition.Definition
import org.koin.core.instance.InstanceFactory
import org.koin.core.module.Module
import org.koin.core.qualifier.named

val sstableQ = named("sstableSegmentManager")

inline fun <reified T> Module.singleSSTableQ(
    createdAtStart: Boolean = false,
    noinline definition: Definition<T>
): Pair<Module, InstanceFactory<T>> = single(sstableQ, createdAtStart, definition)

val sstableModule = sstableOpenSegmentFactories + memTableFactoriesModule + segmentMergeStrategiesModule
