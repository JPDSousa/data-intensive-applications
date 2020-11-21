package org.example.lsm

interface SegmentMergeStrategy<S> {

    fun merge(segmentsToCompact: List<Segment<S>>): List<Segment<S>>
}
