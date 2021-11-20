package org.example.log

import org.koin.core.qualifier.named
import java.nio.file.Path

val sizeLogQ = named(SizeCachedLog::class.qualifiedName!!)

private data class SizeEntry(val size: Int, private val offset: Long, val log: Log<*>) {

    fun isOutdated() = log.lastOffset > this.offset

    // avoid computing the size from the beginning
    fun update() = SizeEntry(
        size + log.useEntries(offset) { it.count() },
        log.lastOffset,
        log
    )

    fun clear() = SizeEntry(
        0,
        0L,
        log
    )

    operator fun inc(): SizeEntry = SizeEntry(
        size + 1,
        log.lastOffset,
        log
    )

    operator fun plus(sizeSuffix: Int) = SizeEntry(
        size + sizeSuffix,
        log.lastOffset,
        log
    )
}

private class SizeCachedLog<T>(
    private val log: Log<T>
): Log<T> by log {

    private var cachedSize = SizeEntry(log.size, log.lastOffset, log)

    override val size: Int
        get() {
            if (cachedSize.isOutdated()) {
                cachedSize = cachedSize.update()
            }

            return cachedSize.size
        }

    override fun clear() {
        log.clear()
        cachedSize = cachedSize.clear()
    }

    override fun append(entry: T) = log.append(entry)
        .also { cachedSize++ }

    override fun appendAll(entries: Sequence<T>): Sequence<Long> {
        var count = 0
        return log.appendAll(entries)
            .onEach { count++ }
            .also { cachedSize += count }
    }
}

class SizeCachedLogFactory<T>(
    private val decoratedFactory: LogFactory<T>
): LogFactory<T> {

    override fun create(logPath: Path): Log<T> = SizeCachedLog(
        decoratedFactory.create(logPath)
    )

}
