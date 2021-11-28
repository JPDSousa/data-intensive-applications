package org.example.kv

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
import org.example.TestInstance
import org.example.defaultPropTestConfig

@DelicateKotest
fun <K, V> keyValueStoreTests(
    gen: Gen<TestInstance<KeyValueStore<K, V>>>,
    keyGen: Arb<K>,
    valueGen: Arb<V>,
    config: PropTestConfig = defaultPropTestConfig,
) = shouldSpec {

    should("absent key") {
        checkAll(config, gen) { instanceSpec ->
            val kv = instanceSpec.instance()
            val key = keyGen.next()

            kv[key] should beNull()
            (key in kv) shouldBe false
        }
    }

    should("written value should be readable") {
        checkAll(config, gen) { instanceSpec ->
            val kv = instanceSpec.instance()
            val key = keyGen.next()
            val expected = valueGen.next()

            kv[key] = expected
            kv[key] shouldBe expected
            (key in kv) shouldBe true
        }
    }

    should("multiple keys are isolated") {
        checkAll(config, gen) { instanceSpec ->
            val kv = instanceSpec.instance()

            val entries = (0..5).associate { Pair(keyGen.next(), valueGen.next()) }

            kv.putAll(entries)

            for (entry in entries) {
                kv[entry.key] shouldBe entry.value
                (entry.key in kv) shouldBe true
            }
        }
    }

    should("key update") {
        checkAll(config, gen) { instanceSpec ->
            val kv = instanceSpec.instance()

            val key = keyGen.next()
            val old = valueGen.next()
            val new = valueGen.next()

            kv[key] = old
            kv[key] = new

            kv[key] shouldBe new
            (key in kv) shouldBe true
        }
    }

    should("deleted key becomes absent") {
        checkAll(config, gen) { instanceSpec ->
            val kv = instanceSpec.instance()

            val distinctKeyGen = keyGen.distinct()
            val key1 = distinctKeyGen.next()
            val value1 = valueGen.next()

            val key2 = distinctKeyGen.next()
            val value2 = valueGen.next()

            kv[key1] = value1
            kv[key2] = value2
            kv.delete(key1)

            kv[key1] should beNull()
            kv[key2] shouldBe value2
            (key1 in kv) shouldBe false
            (key2 in kv) shouldBe true
        }
    }

}

