package org.example

import io.kotest.property.Gen

interface GenWrapper<T> {

    val gen: Gen<T>
}