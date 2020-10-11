package org.example

import org.junit.jupiter.api.extension.*
import java.util.stream.Stream

class CustomParameterizedExtension: TestTemplateInvocationContextProvider {

    override fun supportsTestTemplate(context: ExtensionContext?): Boolean {

        if (context == null) {
            return false
        }

        if (context.testMethod.isPresent.not()) {
            return false
        }

        return true
    }

    override fun provideTestTemplateInvocationContexts(context: ExtensionContext?):
            Stream<TestTemplateInvocationContext> {

        return (context!!.getStore(namespace)[parameters, List::class.java] as List<TemplateInstance>)
                .map { TemplateInvocationContext(it) as TestTemplateInvocationContext }
                .stream()
    }

    companion object {

        val parameters = "parameters"

        val namespace = ExtensionContext.Namespace.create(CustomParameterizedExtension::class)
    }
}

data class TemplateInstance(val name: String, val instance: Any)

private class TemplateInvocationContext(val instance: TemplateInstance) :
        TestTemplateInvocationContext {

    override fun getDisplayName(invocationIndex: Int): String = instance.name

    override fun getAdditionalExtensions(): MutableList<Extension> {
        return mutableListOf(TemplateParameterResolver(instance.instance))
    }
}

private class TemplateParameterResolver(private val instance: Any): ParameterResolver {

    override fun supportsParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?): Boolean
            = true

    override fun resolveParameter(parameterContext: ParameterContext?, extensionContext: ExtensionContext?): Any
            = instance
}
