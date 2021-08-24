package org.example.kv.lsm

import org.example.TestGenerator
import org.example.TestInstance
import org.example.generator.Generator


interface GenericLSMKeyValueStores<K, V>: TestGenerator<LSMKeyValueStore<K, V>>

