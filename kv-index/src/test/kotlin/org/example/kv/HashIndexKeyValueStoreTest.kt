package org.example.kv

import org.example.log.SingleFileLog
import org.junit.jupiter.api.extension.*
import java.nio.file.Path

@ExtendWith(HashIndexKeyValueExtension::class)
internal class HashIndexKeyValueStoreTest: KeyValueStoreTest

internal class HashIndexKeyValueExtension: PathParameterResolverExtension<KeyValueStore>(KeyValueStore::class.java) {

    override fun createParameter(path: Path): KeyValueStore
            = HashIndexKeyValueStore(TextKeyValueStore(SingleFileLog(path)))

}


