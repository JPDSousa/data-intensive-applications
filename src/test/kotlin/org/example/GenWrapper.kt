package org.example

import io.kotest.property.Gen

interface GenWrapper<out T> {

    val gen: Gen<T>
}