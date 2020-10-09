package org.example.kv

import org.junit.jupiter.api.extension.ExtendWith
import java.nio.file.Path

/**
 *
 *
 * @author Joao Sousa (joao.sousa@feedzai.com)
 * @since @@@feedzai.next.release@@@
 */
@ExtendWith(TextKeyValueExtension::class)
class TextKeyValueStoreTest: KeyValueStoreTest

internal class TextKeyValueExtension: PathBasedExtension() {

    override fun createKeyValueStore(path: Path): KeyValueStore = TextKeyValueStore(path)
}
