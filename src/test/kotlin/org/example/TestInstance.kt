package org.example

data class TestInstance<out T>(val name: String, val instance: () -> T)
