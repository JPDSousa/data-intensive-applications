package org.example.trash

import kotlinx.coroutines.CoroutineDispatcher
import org.example.recurrent.OpsBasedRecurrentJob
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import kotlin.math.roundToInt

private const val SAFETY_FACTORY = 1.2

class JobBasedTrash<T>(
    triggerThreshold: Long,
    coroutineDispatcher: CoroutineDispatcher,
    private val delegateTrash: Trash<T>,
): Trash<T> {

    private val itemsToTrash: Queue<T> = ArrayBlockingQueue((triggerThreshold * SAFETY_FACTORY).roundToInt())

    private val job = OpsBasedRecurrentJob(triggerThreshold, coroutineDispatcher) {
        while (itemsToTrash.isNotEmpty()) {
            delegateTrash.mark(itemsToTrash.poll())
        }
    }

    override fun mark(deleteMe: T) {
        itemsToTrash.offer(deleteMe)
        job.registerOperation()
    }
}