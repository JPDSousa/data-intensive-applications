package org.example.concepts

interface SizeMixin {

    val size: Int

}

fun <K, V> Map<K, V>.asSizeMixin(): SizeMixin {

    val map = this

    return object: SizeMixin {

        override val size: Int
            get() = map.size

    }
}