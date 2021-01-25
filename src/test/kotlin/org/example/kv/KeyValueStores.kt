package org.example.kv

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.TestInstance
import org.example.TestResources
import org.example.kv.lsm.LSMKeyValueStoreFactory
import org.example.log.LogFactories

class KeyValueStores(private val iKVs: LogKeyValueStores,
                     private val logFactories: LogFactories,
                     private val resources: TestResources) {

    @ExperimentalSerializationApi
    fun binaryKeyValueStores(): Sequence<TestInstance<KeyValueStore<ByteArray, ByteArray>>> = sequence {

        for (binaryInstance in logFactories.binaryInstances()) {
            for (binaryKeyValueStore in iKVs.binaryKeyValueStores()) {
                yield(TestInstance("String LSM Key Value Store") {
                    val kvDir = resources.allocateTempDir("segmented-")
                    LSMKeyValueStoreFactory(binaryInstance.instance(), binaryKeyValueStore.instance(), Tombstone
                            .byte).createLSMKeyValueStore(kvDir)
                })
            }
        }
    }

    @ExperimentalSerializationApi
    fun stringKeyValueStores(): Sequence<TestInstance<KeyValueStore<String, String>>> = sequence {

        for (stringInstance in logFactories.stringInstances()) {
            for (stringKeyValueStore in iKVs.stringKeyValueStores()) {
                yield(TestInstance("String LSM Key Value Store") {
                    val kvDir = resources.allocateTempDir("segmented-")
                    LSMKeyValueStoreFactory(stringInstance.instance(), stringKeyValueStore.instance(), Tombstone
                            .string).createLSMKeyValueStore(kvDir)
                })
            }
        }
    }

}
