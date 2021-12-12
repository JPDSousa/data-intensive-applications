package org.example.log

import org.example.trash.Trash
import org.koin.core.qualifier.named
import java.nio.file.Path

val sizeLogQ = named(SizeCachedLogFactory.SizeCachedLog::class.qualifiedName!!)

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

class SizeCachedLogFactory<T>(
    private val decoratedFactory: LogFactoryB<T>
): LogFactory<T, SizeCachedLogFactory<T>.SizeCachedLog> {

    inner class SizeCachedLog(
        val log: Log<T>
    ): Log<T> by log {

        private var cachedSize = SizeEntry(log.size, log.lastOffset, log)

        override val size: Int
            get() {
                if (cachedSize.isOutdated()) {
                    cachedSize = cachedSize.update()
                }

                return cachedSize.size
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

    private inner class SizeLogTrash: Trash<SizeCachedLog> {

        override fun mark(deleteMe: SizeCachedLog) = decoratedFactory.trash.mark(deleteMe.log)
    }

    override fun create(logPath: Path): Log<T> = SizeCachedLog(
        decoratedFactory.create(logPath)
    )

    override val trash: Trash<SizeCachedLog> = SizeLogTrash()

}
