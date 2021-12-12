package org.example.trash

import io.kotest.property.Gen
import io.kotest.property.exhaustive.exhaustive
import org.example.GenWrapper
import org.koin.dsl.module
import java.nio.file.Path

data class PathTrashes(override val gen: Gen<Trash<Path>>) : GenWrapper<Trash<Path>>

val trashes = module {

    single { PathTrashes(listOf(PathTrash).exhaustive()) }
}