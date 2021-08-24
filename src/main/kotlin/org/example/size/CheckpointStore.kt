package org.example.size

import java.time.Instant

interface CheckpointStore<T> {

    fun checkpointState(state: T)

    fun <R> useLastState(block: (T?) -> R): R

    val lastInstant: Instant?

}