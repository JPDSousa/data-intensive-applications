package org.example.trash

interface Trash<in T> {

    // TODO add promise
    fun mark(deleteMe: T)
}