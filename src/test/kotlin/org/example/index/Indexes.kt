package org.example.index

import kotlinx.serialization.serializer
import org.example.TestGenerator
import org.example.TestGeneratorAdapter
import org.example.TestInstance
import org.example.encoder.Encoders
import org.example.encoder.ProtobufEncoderGenerator
import org.example.encoder.StringEncoderGenerator
import org.example.generator.CompositeGenerator
import org.example.log.*
import org.koin.dsl.module
import java.util.concurrent.atomic.AtomicLong

val indexesModule = module {

    single<StringIndexEntryLogFactories> {
        DelegateString(composite(
            listOf(
                LogEncoderFactories(
                    get<StringStringEncoders>(),
                    get<StringLogFactories>()
                ),
                LogEncoderFactories(
                    get<StringByteArrayEncoders>(),
                    get<ByteArrayLogFactories>()
                )
            )
        ))
    }

    single<LongIndexEntryLogFactories> {
        DelegateLong(composite(
            listOf(
                LogEncoderFactories(
                    get<LongStringEncoders>(),
                    get<StringLogFactories>()
                ),
                LogEncoderFactories(
                    get<LongByteArrayEncoders>(),
                    get<ByteArrayLogFactories>()
                )
            )
        ))
    }

    single<StringStringEncoders> {
        DelegateStringStringEncoders(
            StringEncoderGenerator(serializer())
        )
    }

    single<StringByteArrayEncoders> {
        DelegateStringByteArrayEncoders(
            ProtobufEncoderGenerator(serializer())
        )
    }

    single<LongStringEncoders> {
        DelegateLongStringEncoders(
            StringEncoderGenerator(serializer())
        )
    }

    single<LongByteArrayEncoders> {
        DelegateLongByteArrayEncoders(
            ProtobufEncoderGenerator(serializer())
        )
    }
}

private fun <K> composite(factories: Iterable<LogFactories<IndexEntry<K>>>) = GenericDelegate(
    TestGeneratorAdapter(
        CompositeGenerator(
            factories
        )
    )
)

interface IndexEntryLogFactories<K>: LogFactories<IndexEntry<K>>
interface StringIndexEntryLogFactories: IndexEntryLogFactories<String>
interface LongIndexEntryLogFactories: IndexEntryLogFactories<Long>

interface StringStringEncoders: Encoders<IndexEntry<String>, String>
interface StringByteArrayEncoders: Encoders<IndexEntry<String>, ByteArray>
interface LongStringEncoders: Encoders<IndexEntry<Long>, String>
interface LongByteArrayEncoders: Encoders<IndexEntry<Long>, ByteArray>

private class DelegateString(private val delegate: IndexEntryLogFactories<String>):
    StringIndexEntryLogFactories, IndexEntryLogFactories<String> by delegate

private class DelegateLong(private val delegate: IndexEntryLogFactories<Long>):
    LongIndexEntryLogFactories, IndexEntryLogFactories<Long> by delegate

private class DelegateStringStringEncoders(private val delegate: Encoders<IndexEntry<String>, String>)
    : StringStringEncoders, Encoders<IndexEntry<String>, String> by delegate

private class DelegateStringByteArrayEncoders(private val delegate: Encoders<IndexEntry<String>, ByteArray>)
    : StringByteArrayEncoders, Encoders<IndexEntry<String>, ByteArray> by delegate

private class DelegateLongStringEncoders(private val delegate: Encoders<IndexEntry<Long>, String>)
    : LongStringEncoders, Encoders<IndexEntry<Long>, String> by delegate

private class DelegateLongByteArrayEncoders(private val delegate: Encoders<IndexEntry<Long>, ByteArray>)
    : LongByteArrayEncoders, Encoders<IndexEntry<Long>, ByteArray> by delegate



private class GenericDelegate<K>(private val downstreamFactories: TestGenerator<LogFactory<IndexEntry<K>>>)
    : IndexEntryLogFactories<K> {

    override fun generate(): Sequence<TestInstance<LogFactory<IndexEntry<K>>>> = downstreamFactories.generate()
}

class Indexes {

    private val generator = AtomicLong()

    fun <K> hashIndexes(): Sequence<TestInstance<Index<K>>> {

        val factory = HashIndexFactory<K>()

        val volatileHash = TestInstance("Hash Index") {
            factory.create("HashIndex${generator.getAndIncrement()}")
        }

        return sequenceOf(volatileHash)
    }

    fun <K: Comparable<K>> treeIndexes(): Sequence<TestInstance<Index<K>>> {

        val factory = TreeIndexFactory<K>()
        val transientTree = TestInstance("Tree Index") {
            factory.create("TreeIndex${generator.getAndIncrement()}")
        }

        return sequenceOf(transientTree)
    }

}
