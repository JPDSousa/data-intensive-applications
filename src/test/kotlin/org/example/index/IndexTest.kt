package org.example.index

import io.kotest.core.spec.style.shouldSpec
import io.kotest.matchers.nulls.beNull
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.next
import io.kotest.property.checkAll
import org.example.TestInstance

fun <K> indexTests(
    indices: Gen<TestInstance<Index<K>>>,
    keyGen: Arb<K>,
    config: PropTestConfig = PropTestConfig(maxFailure = 3, iterations = 100),
) = shouldSpec {

    should("offsets are persisted") {
        checkAll(config, indices) { spec ->
            val index = spec.instance()
            val key = keyGen.next()
            val expected = 1234L

            index[key] = expected
            index[key] shouldBe expected
        }
    }

    should("absent entry has no offset") {
        checkAll(config, indices) { spec ->
            val index = spec.instance()

            index[keyGen.next()] should beNull()
        }
    }

    should("reads do not delete entries") {
        checkAll(config, indices) { spec ->
            val index = spec.instance()
            val key = keyGen.next()
            val expected = 1234L

            index[key] = expected
            index[key] shouldBe expected
            // second read makes sure that the value is still there
            index[key] shouldBe expected
        }
    }

    should("sequential writes act as updates") {
        checkAll(config, indices) { spec ->
            val index = spec.instance()
            val key = keyGen.next()
            val expected = 4321L
            index[key] = 1234L
            index[key] = expected

            index[key] shouldBe expected
        }
    }

    should("keys are isolated") {
        checkAll(config, indices) { spec ->
            val index = spec.instance()
            val key1 = keyGen.next()
            val value1 = 1234L
            val key2 = keyGen.next()
            val value2 = 4321L
            index[key1] = value1
            index[key2] = value2

            index[key1] shouldBe value1
            index[key2] shouldBe value2
        }
    }
}

@Suppress("FunctionName")
interface IndexTest<K> {

    fun instances(): Sequence<TestInstance<Index<K>>>

    fun nextKey(): K

}
