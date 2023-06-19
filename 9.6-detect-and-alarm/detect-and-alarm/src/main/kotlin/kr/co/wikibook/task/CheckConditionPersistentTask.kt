package kr.co.wikibook.task

import org.elasticsearch.persistent.AllocatedPersistentTask
import org.elasticsearch.tasks.TaskId
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture

class CheckConditionPersistentTask(
    id: Long,
    type: String,
    action: String,
    description: String,
    parentTask: TaskId,
    headers: MutableMap<String, String>
) : AllocatedPersistentTask(id, type, action, description, parentTask, headers) {
    var future: ScheduledFuture<*>? = null

    override fun onCancelled() {
        super.onCancelled()
        future?.cancel(true)
        markAsCompleted()
    }
}