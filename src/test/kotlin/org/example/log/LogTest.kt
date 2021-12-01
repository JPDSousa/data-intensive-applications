package org.example.log

import io.kotest.core.spec.style.shouldSpec
import io.kotest.matchers.collections.beEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.longs.shouldBeLessThan
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.Gen
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.next
import io.kotest.property.checkAll
import org.example.defaultPropTestConfig

fun <T> logTests(
    gen: Gen<Log<T>>,
    valueGen: Arb<T>,
    config: PropTestConfig = defaultPropTestConfig,
) = shouldSpec {

    should("append on empty file should return 0") {
        checkAll(config, gen) { log ->
            val initialSize = log.byteLength
            log.append(valueGen.next()) shouldBe initialSize
        }
    }

    should("append should sum offset") {
        checkAll(config, gen) { log ->
            val firstOffset = log.append(valueGen.next())
            val secondOffset = log.append(valueGen.next())

            firstOffset shouldBeLessThan secondOffset
        }
    }

    should("single append should be readable") {
        checkAll(config, gen) { log ->
            val initialSize = log.useEntries { it.toList() }.size

            val expected = valueGen.next()
            log.append(expected)

            val items = log.useEntries { it.toList() }

            items.size shouldBe initialSize + 1

            items.last() shouldBe expected
        }
    }

    fun multipleReadWriteCycle(log: Log<T>, countValues: Int = 50, append: (Sequence<T>) -> Sequence<Long>) {

        val initialSize = log.useEntries { it.count() }

        val expectedList = generateSequence { valueGen.next() }
            .take(countValues)
            .toList()

        val insertedOffsets = append(expectedList.asSequence()).toList()

        val actualValuesList = log.useEntriesWithOffset { it.toList() }

        insertedOffsets shouldHaveSize expectedList.size
        actualValuesList shouldHaveSize initialSize + expectedList.size

        val testItems = actualValuesList.subList(initialSize, actualValuesList.size)

        expectedList.zip(testItems).forEach {

            it.second.entry shouldBe it.first
        }

        testItems.zip(insertedOffsets).forEach {

            it.first.offset shouldBe it.second
        }
    }

    should("multiple appends should be readable") {
        checkAll(config, gen) { log ->
            multipleReadWriteCycle(log) { values -> values.map { log.append(it) }}
        }
    }

    should("atomic multiple appends (appendAll) should be readable") {
        checkAll(config, gen) { log ->
            multipleReadWriteCycle(log) { values -> log.appendAll(values) }
        }
    }

    should("empty appendAll should not change structure") {
        checkAll(config, gen) { log ->
            multipleReadWriteCycle(log, 0) { values -> log.appendAll(values) }
        }
    }

    should("clear removes all entries") {
        checkAll(config, gen) { log ->
            log.clear()
            log.byteLength shouldBe 0L
            log.useEntries { it.toList() } should beEmpty()
        }
    }
}
