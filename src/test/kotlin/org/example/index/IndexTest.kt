package org.example.index

import io.kotest.common.DelicateKotest
import io.kotest.core.spec.style.shouldSpec
import io.kotest.matchers.nulls.beNull
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.distinct
import io.kotest.property.arbitrary.next
import io.kotest.property.checkAll
import org.example.defaultPropTestConfig
import org.example.randomSource

@DelicateKotest
fun <K> indexTests(
    indices: Gen<Index<K>>,
    keyGen: Arb<K>,
    config: PropTestConfig = defaultPropTestConfig,
) = shouldSpec {

    val rs = config.randomSource()
    should("offsets are persisted") {
        checkAll(config, indices) { index ->
            val key = keyGen.next(rs)
            val expected = 1234L

            index[key] = expected
            index[key] shouldBe expected
        }
    }

    should("absent entry has no offset") {
        checkAll(config, indices) { index ->

            index[keyGen.next(rs)] should beNull()
        }
    }

    should("reads do not delete entries") {
        checkAll(config, indices) { index ->
            val key = keyGen.next(rs)
            val expected = 1234L

            index[key] = expected
            index[key] shouldBe expected
            // second read makes sure that the value is still there
            index[key] shouldBe expected
        }
    }

    should("sequential writes act as updates") {
        checkAll(config, indices) { index ->
            val key = keyGen.next(rs)
            val expected = 4321L
            index[key] = 1234L
            index[key] = expected

            index[key] shouldBe expected
        }
    }

    should("keys are isolated") {
        checkAll(config, indices) { index ->
            val distinctKeyGen = keyGen.distinct()
            val key1 = distinctKeyGen.next(rs)
            val value1 = 1234L
            val key2 = distinctKeyGen.next(rs)
            val value2 = 4321L
            index[key1] = value1
            index[key2] = value2

            index[key1] shouldBe value1
            index[key2] shouldBe value2
        }
    }
}
