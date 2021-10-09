package org.example.kv.lsm.sstable

import org.example.TestGenerator
import org.example.TestInstance
import org.example.kv.LogKeyValueStoreFactories
import org.example.kv.LongByteArrayLogKeyValueStoreFactories
import org.example.kv.StringStringLogKeyValueStoreFactories
import org.example.size.ByteArraySizeCalculator
import org.example.size.LongSizeCalculator
import org.example.size.SizeCalculator
import org.example.size.StringSizeCalculator
import org.koin.dsl.module

interface MemTableFactories<K: Comparable<K>, V>: TestGenerator<MemTableFactory<K, V>>

interface StringStringMemTableFactories: MemTableFactories<String, String>
interface LongByteArrayMemTableFactories: MemTableFactories<Long, ByteArray>

private class DelegateStringStringMemTableFactories(private val delegate: MemTableFactories<String, String>)
    : StringStringMemTableFactories, MemTableFactories<String, String> by delegate

private class DelegateLongByteArrayMemTableFactories(private val delegate: MemTableFactories<Long, ByteArray>)
    : LongByteArrayMemTableFactories, MemTableFactories<Long, ByteArray> by delegate

private class GenericMemTableFactories<K: Comparable<K>, V>(
    private val logKeyValueStoreFactories: LogKeyValueStoreFactories<K, V>,
    private val keySize: SizeCalculator<K>,
    private val valueSize: SizeCalculator<V>
): MemTableFactories<K, V> {

    override fun generate(): Sequence<TestInstance<MemTableFactory<K, V>>> = sequence {

        for (logKeyValueStoreFactory in logKeyValueStoreFactories) {
            yield(TestInstance("${MemTableFactory::class.simpleName} with $logKeyValueStoreFactory, " +
                    "$keySize and $valueSize") {
                MemTableFactory(
                    logKeyValueStoreFactory.instance(),
                    keySize,
                    valueSize
                )
            })
        }

    }

}

val memTableFactoriesModule = module {

    single<StringStringMemTableFactories> {
        DelegateStringStringMemTableFactories(
            GenericMemTableFactories(
                get<StringStringLogKeyValueStoreFactories>(),
                get<StringSizeCalculator>(),
                get<StringSizeCalculator>()
            )
        )
    }

    single<LongByteArrayMemTableFactories> {
        DelegateLongByteArrayMemTableFactories(
            GenericMemTableFactories(
                get<LongByteArrayLogKeyValueStoreFactories>(),
                get<LongSizeCalculator>(),
                get<ByteArraySizeCalculator>()
            )
        )
    }
}
