package org.example.kv

import org.example.kv.lsm.lsmModule

val kvModule = lsmModule + keyValueStoresModule + logKeyValueStoreFactoryModule