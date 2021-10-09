package org.example.kv.lsm

import org.example.TestGenerator
import org.example.TestInstance
import org.example.TestResources
import org.koin.dsl.module

interface SegmentDirectories: TestGenerator<SegmentDirectory>

private class GenericSegmentDirectories(private val resources: TestResources): SegmentDirectories {

    override fun generate(): Sequence<TestInstance<SegmentDirectory>> = sequenceOf(
        TestInstance("${SegmentDirectory::class.simpleName}") {
            SegmentDirectory(resources.allocateTempDir("segmented-"))
        }
    )
}

val segmentDirectoriesModule = module {

    single<SegmentDirectories> {
        GenericSegmentDirectories(get())
    }
}