package org.example.kv

import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver

@ExtendWith(MemoryKeyValueExtension::class)
internal class MemoryKeyValueStoreTest: KeyValueStoreTest

internal class MemoryKeyValueExtension: TypeBasedParameterResolver<KeyValueStore>() {

    override fun resolveParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?):
            KeyValueStore = MemoryKeyValueStore()
}
