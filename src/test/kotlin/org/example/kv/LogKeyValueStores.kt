package org.example.kv

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.serializer
import org.example.TestInstance
import org.example.index.IndexFactories
import org.example.index.IndexFactory

class LogKeyValueStores(private val indexFactories: IndexFactories) {

    @ExperimentalSerializationApi
    fun stringKeyValueStores(): Sequence<TestInstance<LogBasedKeyValueStoreFactory<String, String>>> {

        val singleKVs = singleKeyValueStores<String, String>(Tombstone.string).toList()

        return singleKVs.asSequence() + indexedKeyValueStores(
            indexFactories.comparableInstances(serializer()),
            singleKVs,
            Tombstone.string)
    }

    @ExperimentalSerializationApi
    fun binaryKeyValueStores(): Sequence<TestInstance<LogBasedKeyValueStoreFactory<ByteArray, ByteArray>>> {

        val singleKVs = singleKeyValueStores<ByteArray, ByteArray>(Tombstone.byte).toList()

        return singleKVs.asSequence() + indexedKeyValueStores(
                indexFactories.instances(serializer()),
                singleKVs,
                Tombstone.byte
        )
    }

    @ExperimentalSerializationApi
    private fun <K, V> singleKeyValueStores(tombstone: V): Sequence<TestInstance<LogBasedKeyValueStoreFactory<K, V>>>
    = sequenceOf(
        TestInstance("Single log key value store") {
            SingleLogKeyValueStoreFactory(tombstone)
        }
    )

    @ExperimentalSerializationApi
    private fun <K, V> indexedKeyValueStores(sequence: Sequence<TestInstance<IndexFactory<K>>>,
                                             lKVs: List<TestInstance<LogBasedKeyValueStoreFactory<K, V>>>,
                                             tombstone: V):
            Sequence<TestInstance<LogBasedKeyValueStoreFactory<K, V>>> = sequence.flatMap {

        lKVs.map { lKV ->
            TestInstance("Index KV with string LogKV ~ ${it.name}") {
                IndexedKeyValueStoreFactory(it.instance(), tombstone, lKV.instance())
            }
        }
    }

}
