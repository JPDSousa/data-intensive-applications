package org.example.kv

class HashIndexKeyValueStore(private val kv: SeekableKeyValueStore): KeyValueStore {

    private val index = mutableMapOf<String, Long>()

    override fun put(key: String, value: String) {
        index[key] = kv.putAndGetOffset(key, value)
    }

    override fun get(key: String): String? {
        val offset = index[key]

        if (offset != null) {
            return kv.get(key, offset)
        }

        return kv.getWithOffset(key)
                ?.also { index[it.second] = it.first }
                ?.second
    }
}
