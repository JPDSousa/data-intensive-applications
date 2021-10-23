package org.example.kv.lsm

import org.example.kv.lsm.sequential.sequentialModule
import org.example.kv.lsm.sstable.sstableModule

val lsmModule = sequentialModule + sstableModule + segmentManagersModule + keyValueStoreFactoriesModule