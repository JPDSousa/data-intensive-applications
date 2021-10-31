package org.example.kv.lsm

// TODO implement size-tiered and leveled compressing operations
interface SegmentMergeStrategy<K, V> {

    fun merge(segmentsToCompact: List<Segment<K, V>>): List<Segment<K, V>>
}
