package org.example.kv

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.extension.*
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver
import java.nio.file.Files
import java.nio.file.Path

/**
 *
 *
 * @author Joao Sousa (joao.sousa@feedzai.com)
 * @since @@@feedzai.next.release@@@
 */
@ExtendWith(HashIndexKeyValueExtension::class)
internal class HashIndexKeyValueStoreTest: KeyValueStoreTest

internal class HashIndexKeyValueExtension: PathBasedExtension() {

    override fun createKeyValueStore(path: Path): KeyValueStore = HashIndexKeyValueStore(TextKeyValueStore(path))
}


