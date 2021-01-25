package org.example.kv

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.serializer
import org.example.TestInstance
import org.example.encoder.Encoder
import org.example.encoder.Encoders
import org.example.index.IndexFactories
import org.example.index.IndexFactory

class LogKeyValueStores(private val encoders: Encoders,
                        private val indexFactories: IndexFactories) {

    @ExperimentalSerializationApi
    fun stringKeyValueStores(): Sequence<TestInstance<LogBasedKeyValueStoreFactory<String, String, String>>> {

        val encoders = encoders.strings(serializer<Pair<String, String>>())
        val singleKVs = singleKeyValueStores(encoders, Tombstone.string).toList()

        return singleKVs.asSequence() +
                indexedKeyValueStores(indexFactories.comparableInstances(serializer()), singleKVs,  Tombstone.string)
    }

    @ExperimentalSerializationApi
    fun binaryKeyValueStores(): Sequence<TestInstance<LogBasedKeyValueStoreFactory<ByteArray, ByteArray, ByteArray>>> {

        val encoders = encoders.binaries<Pair<ByteArray, ByteArray>>(serializer())
        val singleKVs = singleKeyValueStores(encoders, Tombstone.byte).toList()

        return singleKVs.asSequence() + indexedKeyValueStores(
                indexFactories.instances(serializer()),
                singleKVs,
                Tombstone.byte
        )
    }

    @ExperimentalSerializationApi
    private fun <E, K, V> singleKeyValueStores(sequence: Sequence<TestInstance<Encoder<Pair<K, V>, E>>>,
                                               tombstone: V):
            Sequence<TestInstance<LogBasedKeyValueStoreFactory<E, K, V>>> = sequence.map {
        TestInstance("Single log key value store") {
            SingleLogKeyValueStoreFactory(tombstone, it.instance())
        }
    }

    @ExperimentalSerializationApi
    private fun <E, K, V>
            indexedKeyValueStores(sequence: Sequence<TestInstance<IndexFactory<K>>>,
                                  lKVs: List<TestInstance<LogBasedKeyValueStoreFactory<E, K, V>>>,
                                  tombstone: V)
            : Sequence<TestInstance<LogBasedKeyValueStoreFactory<E, K, V>>> = sequence.flatMap {

        lKVs.map { lKV ->
            TestInstance("Index KV with string LogKV ~ ${it.name}") {
                IndexedKeyValueStoreFactory(it.instance(), tombstone, lKV.instance())
            }
        }
    }

}
