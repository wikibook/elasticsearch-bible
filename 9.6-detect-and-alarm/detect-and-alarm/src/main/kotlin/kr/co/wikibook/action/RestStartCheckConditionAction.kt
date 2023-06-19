package kr.co.wikibook.action

import org.elasticsearch.client.internal.node.NodeClient
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.action.RestStatusToXContentListener

class RestStartCheckConditionAction : BaseRestHandler() {
    override fun routes(): MutableList<RestHandler.Route> = mutableListOf(
        RestHandler.Route(RestRequest.Method.POST, "_detect_and_alarm/_start")
    )

    override fun getName(): String = "detect_and_alarm"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val fixedDelaySec = request.paramAsLong("fixedDelaySec", 5L)
        val actionRequest = StartCheckConditionRequest(fixedDelaySec)

        return RestChannelConsumer { channel ->
            val listener = RestStatusToXContentListener<StartCheckConditionResponse>(channel)
            client.execute(StartCheckConditionAction, actionRequest, listener)
        }
    }
}