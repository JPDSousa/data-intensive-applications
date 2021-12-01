package org.example.kv.lsm

import io.kotest.property.Gen
import io.kotest.property.arbitrary.arbitrary
import org.example.GenWrapper
import org.example.TestResources
import org.koin.dsl.module

data class SegmentDirectories(
    override val gen: Gen<SegmentDirectory>
) : GenWrapper<SegmentDirectory>

val segmentDirectoriesModule = module {

    single {
        val resources: TestResources = get()
        SegmentDirectories(
            arbitrary { SegmentDirectory(resources.allocateTempDir("segmented-")) }
        ) }
}