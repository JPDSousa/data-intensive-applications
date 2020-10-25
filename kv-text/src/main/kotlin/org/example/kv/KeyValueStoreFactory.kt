package org.example.kv

import kotlinx.serialization.ExperimentalSerializationApi
import org.example.index.IndexFactory
import org.example.log.BinaryLog
import org.example.log.Index
import org.example.log.Log
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files.createFile
import java.nio.file.Path

class KeyValueStoreFactory {

    fun createStringKeyValueStore(index: Index<String>, log: Log<String>) = createStringKeyValueStore(
            index,
            log,
            CSVEncoder({ it }, { it }))

    fun createStringKeyValueStore(index: Index<String>, log: Log<ByteArray>, charset: Charset = UTF_8) =
            createStringKeyValueStore(
            index,
            log,
            CSVEncoder({ it.toByteArray(charset) }, { String(it, charset) })
    )

    fun <E> createStringKeyValueStore(index: Index<String>,
                                      log: Log<E>,
                                      encoder: EntryEncoder<E, String, String>): KeyValueStore<String, String> {
        return IndexedKeyValueStore(
                index,
                log,
                encoder,
                tombstoneStr
        )
    }

    fun createBinaryKeyValueStore(index: Index<ByteArray>,
                                  log: Log<String>,
                                  charset: Charset = UTF_8) = createBinaryKeyValueStore(
            index,
            log,
            BinaryEncoder({ String(it, charset) }, { it.toByteArray(charset) }))

    fun createBinaryKeyValueStore(index: Index<ByteArray>, log: Log<ByteArray>) =
            createBinaryKeyValueStore(
                    index,
                    log,
                    BinaryEncoder({ it }, { it })
            )

    fun <E> createBinaryKeyValueStore(
            index: Index<ByteArray>,
            log: Log<E>,
            encoder: EntryEncoder<E, ByteArray, ByteArray>): KeyValueStore<ByteArray, ByteArray> {

        return IndexedKeyValueStore(
                index,
                log,
                encoder,
                tombstoneByte
        )
    }

    fun createSegmentedStringKeyValueStore(kvDir: Path): KeyValueStore<String, String> = SegmentedKeyValueStore(
            SegmentFactory(StringResources(kvDir, tombstoneStr)),
            tombstoneStr
    )

    fun createSegmentedBinaryKeyValueStore(kvDir: Path): KeyValueStore<ByteArray, ByteArray> = SegmentedKeyValueStore(
            SegmentFactory(BinaryResources(kvDir, tombstoneByte)),
            tombstoneByte
    )

    companion object {

        private const val tombstoneStr = "tombstone"
        private val tombstoneByte = ByteArray(0)

    }
}

private class BinaryResources(
        private val kvDir: Path,
        private val tombstone: ByteArray): SegmentResourcesFactory<ByteArray, ByteArray, ByteArray> {

    private val indexFactory = IndexFactory()

    override fun createLog(segmentId: String) = "segment-log-$segmentId.log"
            .let { kvDir.resolve(it) }
            .let { createFile(it) }
            .let { BinaryLog(it) }

    @ExperimentalSerializationApi
    override fun createIndex(segmentId: String) = "segment-index-$segmentId.log"
            .let { kvDir.resolve(it) }
            .let { createFile(it) }
            .let { BinaryLog(it) }
            .let { indexFactory.createBinaryLogEncoder<ByteArray>(it) }
            .let { indexFactory.createTreeIndex(it) }

    override fun createEncoder() = BinaryEncoder({ it }, { it })

    override fun tombstone() = tombstone

}

// TODO inject index and log creation
private class StringResources(
        private val kvDir: Path,
        private val tombstone: String,
        private val charset: Charset = UTF_8): SegmentResourcesFactory<ByteArray, String, String> {

    private val indexFactory = IndexFactory()

    override fun createLog(segmentId: String) = "segment-log-$segmentId.log"
            .let { kvDir.resolve(it) }
            .let { createFile(it) }
            .let { BinaryLog(it) }

    @ExperimentalSerializationApi
    override fun createIndex(segmentId: String) = "segment-index-$segmentId.log"
            .let { kvDir.resolve(it) }
            .let { createFile(it) }
            .let { BinaryLog(it) }
            .let { indexFactory.createBinaryLogEncoder<String>(it) }
            .let { indexFactory.createTreeIndex(it) }

    override fun createEncoder(): EntryEncoder<ByteArray, String, String>
            = CSVEncoder({ it.toByteArray(charset) }, { String(it, charset) })

    override fun tombstone() = tombstone

}
