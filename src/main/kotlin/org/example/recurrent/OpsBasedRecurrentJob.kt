package org.example.recurrent

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import mu.KotlinLogging

class OpsBasedRecurrentJob(
    private val triggerThreshold: Long,
    private val coroutineDispatcher: CoroutineDispatcher,
    private val executable: () -> Unit
): RecurrentJob {

    private val logger = KotlinLogging.logger {}

    @Volatile
    private var opsWithoutRunning: Long = 0

    override fun registerOperation() {
        opsWithoutRunning++
        if (opsWithoutRunning >= triggerThreshold) {
            // TODO make sure we don't trigger more than one job at the same time
            GlobalScope.launch(coroutineDispatcher) {
                logger.debug { "Triggering job at $opsWithoutRunning operations" }
                val opsWithoutRunning = this@OpsBasedRecurrentJob.opsWithoutRunning
                executable()
                this@OpsBasedRecurrentJob.opsWithoutRunning -= opsWithoutRunning
            }
        }
    }

    override fun reset() {
        opsWithoutRunning = 0
    }
}
