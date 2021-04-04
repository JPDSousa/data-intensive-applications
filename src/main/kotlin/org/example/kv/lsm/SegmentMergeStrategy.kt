package org.example.kv.lsm

interface SegmentMergeStrategy<K, V> {

    fun merge(segmentsToCompact: List<Segment<K, V>>): List<Segment<K, V>>
}
