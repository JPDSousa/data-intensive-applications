package org.example.kv

import java.nio.ByteBuffer

interface EntryEncoder<E, K, V> {

    fun encode(key: K, value: V): E

    fun encode(entry: Pair<K, V>) = encode(entry.first, entry.second)

    fun decode(entry: E): Pair<K, V>

}

class CSVEncoder<E>(private val encoder: (String) -> E,
                    private val decoder: (E) -> String): EntryEncoder<E, String, String> {

    private val regex = Regex(",")

    override fun encode(key: String, value: String) = encoder("$key,$value")

    override fun decode(entry: E): Pair<String, String> {
        val raw = regex.split(decoder(entry), 2)

        return Pair(raw[0], raw[1])
    }

}

class BinaryEncoder<E>(private val encoder: (ByteArray) -> E,
                       private val decoder: (E) -> ByteArray): EntryEncoder<E, ByteArray, ByteArray> {

    override fun encode(key: ByteArray, value: ByteArray): E {
        val buffer = ByteBuffer.allocate(8 + key.size + value.size)
        buffer.putInt(key.size)
        buffer.putInt(value.size)
        buffer.put(key)
        buffer.put(value)

        return encoder(buffer.array())
    }

    override fun decode(entry: E): Pair<ByteArray, ByteArray> {
        val buffer = ByteBuffer.wrap(decoder(entry))

        val keySize = buffer.getInt(0)

        val valueSize = buffer.getInt(4)

        buffer.position(8)
        val key = ByteArray(keySize)
        buffer.get(key, 0, keySize)

        val value = ByteArray(valueSize)
        buffer.get(value, 0, valueSize)

        return Pair(key, value)
    }

}
