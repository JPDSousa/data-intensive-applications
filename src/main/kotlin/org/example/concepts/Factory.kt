package org.example.concepts

import kotlin.reflect.KClass

/**
 * A factory type for [creatingClass].
 */
annotation class Factory(val creatingClass: KClass<*>)
