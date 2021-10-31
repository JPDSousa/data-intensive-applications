package org.example.concepts

interface SerializationMixin {

    @Read(Cardinality.ONE) val byteLength: Long

}