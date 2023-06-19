package kr.co.wikibook.action

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.TransportAction
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportHelloWorldAction @Inject constructor(
    actionFilters: ActionFilters, transportService: TransportService,
    private val settings: Settings
) : TransportAction<HelloWorldRequest, HelloWorldResponse>(
    NAME, actionFilters, transportService.taskManager
) {
    override fun doExecute(task: Task, request: HelloWorldRequest,
                           listener: ActionListener<HelloWorldResponse>) {
        try {
            val greetings = settings.get("hello.greetings", "hello")
            listener.onResponse(HelloWorldResponse(greetings, request.name))
        } catch (e: Exception) {
            listener.onFailure(e)
        }
    }
}