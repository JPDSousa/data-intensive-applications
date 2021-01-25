package org.example

fun possiblyArrayEquals(val1: Any?, val2: Any?): Boolean {

    if (val1 is ByteArray && val2 is ByteArray) {
        return val1.contentEquals(val2)
    }

    return val1 == val2
}
