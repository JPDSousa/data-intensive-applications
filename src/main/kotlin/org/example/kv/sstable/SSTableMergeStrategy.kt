package org.example.kv.sstable

import org.example.lsm.Segment
import org.example.lsm.SegmentFactory
import org.example.lsm.SegmentMergeStrategy
import java.util.*
import kotlin.collections.ArrayList

class SSTableMergeStrategy<K: Comparable<K>, V>(
    private val segmentFactory: SegmentFactory<K, V>)
    : SegmentMergeStrategy<K, V> {

    override fun merge(segmentsToCompact: List<Segment<K, V>>)
    : List<Segment<K, V>> = mergeSort(segmentsToCompact, 0, ArrayList(segmentsToCompact.size))

    private fun mergeSort(segments: List<Segment<K, V>>,
                          index: Int,
                          sequences: MutableList<Sequence<Map.Entry<K, V>>>): List<Segment<K, V>> {

        if (index < segments.size) {
            return segments[index].log.useEntries {
                sequences.add(it)
                return@useEntries mergeSort(segments, index + 1, sequences)
            }
        }

        val compactedSegments: MutableList<Segment<K, V>> = LinkedList()
        var openSegment = segmentFactory.createSegment()
        // merge sort starts here
        val headElements: MutableList<Map.Entry<K, V>?> = ArrayList(sequences.size)
        for (sequence in sequences) {
            headElements.add(sequence.firstOrNull())
        }
        while (headElements.any { it != null }) {
            if (openSegment.isFull()) {
                compactedSegments.add(openSegment)
                openSegment = segmentFactory.createSegment()
            }

            var min: Map.Entry<K, V>? = null
            var minI = -1
            for (i in sequences.indices) {
                if (headElements[i] == null) {
                    headElements[i] = sequences[i].firstOrNull()
                }
                if (headElements[i] != null) {
                    if (min == null) {
                        min = headElements[i]
                        minI = i
                    } else if (headElements[i]!!.key < min.key) {
                        min = headElements[i]
                        minI = i
                    }
                }
            }
            if (min != null) {
                openSegment.log.append(min)
                if (minI >= 0) {
                    headElements[minI] = null
                }
            }
        }

        return compactedSegments
    }
}
