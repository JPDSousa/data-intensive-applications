package org.example.kv.lsm

import org.example.kv.lsm.sequential.sequentialModule
import org.example.kv.lsm.sequential.sequentialQ
import org.example.kv.lsm.sstable.sstableModule
import org.example.kv.lsm.sstable.sstableQ

val qualifiers = listOf(sequentialQ, sstableQ)

val lsmModule = sequentialModule +
        sstableModule +
        segmentManagersModule +
        keyValueStoreFactoriesModule +
        lsmSegmentMergeStrategies +
        segmentDirectoriesModule +
        openSegmentFactoriesModule +
        segmentFactoriesModule

