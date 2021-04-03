package org.example.size

interface SizeCalculator<E> {

    fun sizeOf(value: E): Int

}
