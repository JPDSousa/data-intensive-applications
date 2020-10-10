package org.example.kv

import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
import java.nio.file.Path

abstract class PathParameterResolverExtension<T>(private val clazz: Class<T>, isFile: Boolean = true):
        PathStoreExtension(isFile), ParameterResolver {

    abstract fun createParameter(path: Path): T

    override fun resolveParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?):
            T? = createParameter(extensionContext!!.getStore().getPath(extensionContext))

    override fun supportsParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?): Boolean
            = parameterContext!!.parameter.type == clazz

}
