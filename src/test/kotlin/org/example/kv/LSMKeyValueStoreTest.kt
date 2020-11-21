package org.example.kv

import java.util.concurrent.atomic.AtomicLong

internal abstract class LSMKeyValueStoreTest<K, V>: KeyValueStoreTest<K, V> {

    internal val kvs = KeyValueStores()
    internal val uniqueGenerator = AtomicLong()

}

internal class StringLSMKeyValueStoreTest: LSMKeyValueStoreTest<String, String>() {

    override fun instances() = kvs.stringSegmentedKeyValueStores()

    override fun nextKey() = uniqueGenerator.getAndIncrement()
            .toString()

    override fun nextValue() = uniqueGenerator.getAndIncrement()
            .toString()


}

internal class BinaryLSMKeyValueStoreTest: LSMKeyValueStoreTest<ByteArray, ByteArray>() {

    override fun instances() = kvs.binarySegmentedKeyValueStores()

    override fun nextKey() = uniqueGenerator.getAndIncrement()
            .toString()
            .toByteArray()

    override fun nextValue() = uniqueGenerator.getAndIncrement()
            .toString()
            .toByteArray()

}
