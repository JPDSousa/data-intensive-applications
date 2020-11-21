package org.example.lsm

interface Segment<S> {

    fun isClosed(): Boolean

    fun clear()

    val structure: S

    val size: Long

}

interface SegmentFactory<S> {

    fun createSegment(): Segment<S>

}
