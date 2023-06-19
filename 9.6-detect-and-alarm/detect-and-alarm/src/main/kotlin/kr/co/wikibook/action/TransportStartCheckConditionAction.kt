package kr.co.wikibook.action

import kr.co.wikibook.task.CheckConditionPersistentTaskParams
import kr.co.wikibook.task.CheckConditionPersistentTaskParams.Companion.TASK_NAME
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.TransportAction
import org.elasticsearch.common.UUIDs
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask
import org.elasticsearch.persistent.PersistentTasksService
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportStartCheckConditionAction @Inject constructor(
    actionFilters: ActionFilters,
    transportService: TransportService,
    private val persistentTasksService: PersistentTasksService
) : TransportAction<StartCheckConditionRequest, StartCheckConditionResponse>(
    NAME, actionFilters, transportService.taskManager
) {
    override fun doExecute(task: Task, request: StartCheckConditionRequest, listener: ActionListener<StartCheckConditionResponse>) {
        val taskParams = CheckConditionPersistentTaskParams(request.fixedDelaySec)
        persistentTasksService.sendStartRequest(UUIDs.randomBase64UUID() + task.id, TASK_NAME, taskParams, object : ActionListener<PersistentTask<CheckConditionPersistentTaskParams>> {
            override fun onResponse(response: PersistentTask<CheckConditionPersistentTaskParams>) {
                listener.onResponse(StartCheckConditionResponse())
            }

            override fun onFailure(e: Exception) {
                listener.onFailure(e)
            }
        })
    }
}