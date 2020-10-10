package org.example.kv

import org.example.log.SegmentedLog
import org.example.log.SingleFileLog
import org.junit.jupiter.api.extension.*
import java.nio.file.Path

@ExtendWith(HashIndexKeyValueExtension::class)
internal class HashIndexKeyValueStoreTest: KeyValueStoreTest

internal class HashIndexKeyValueExtension: PathParameterResolverExtension<KeyValueStore>(KeyValueStore::class.java,
        false) {

    override fun createParameter(path: Path): KeyValueStore
            = HashIndexKeyValueStore(TextKeyValueStore(SegmentedLog(path)))

}


