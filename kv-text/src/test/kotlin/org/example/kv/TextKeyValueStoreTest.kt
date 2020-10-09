package org.example.kv

import org.example.log.SingleFileLog
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

internal class TextKeyValueExtension: PathBasedExtension<KeyValueStore>(KeyValueStore::class.java) {

    override fun createParameter(path: Path): KeyValueStore
            = TextKeyValueStore(SingleFileLog(path))
}
