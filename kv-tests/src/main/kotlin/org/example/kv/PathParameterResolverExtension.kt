package org.example.kv

import org.example.PathStoreExtension
import org.example.getPath
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
import java.nio.file.Path

abstract class PathParameterResolverExtension<T>(private val clazz: Class<T>):
        PathStoreExtension, ParameterResolver {

    abstract fun createParameter(path: Path): T

    override fun resolveParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?):
            T? = createParameter(extensionContext!!.getPathStore().getPath(extensionContext))

    override fun supportsParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?): Boolean
            = parameterContext!!.parameter.type == clazz

}
