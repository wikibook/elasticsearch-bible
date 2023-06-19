package kr.co.wikibook.rest

import kr.co.wikibook.action.PartialDumpAction
import kr.co.wikibook.action.PartialDumpRequest
import kr.co.wikibook.action.PartialDumpResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.internal.node.NodeClient
import org.elasticsearch.common.Strings
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestHandler.Route
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestRequest.Method.POST
import org.elasticsearch.rest.action.RestStatusToXContentListener
import org.elasticsearch.search.builder.SearchSourceBuilder

class RestPartialDumpAction : BaseRestHandler() {
    override fun routes(): MutableList<Route> = mutableListOf(
        Route(POST, "/_dump/{index}")
    )

    override fun getName() = "partial_dump_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val searchRequest = SearchRequest()

        val indices = Strings.splitStringByCommaToArray(request.param("index"))
        searchRequest.indices(*indices)
        searchRequest.routing(request.param("routing"))

        val source = SearchSourceBuilder()
        source.parseXContent(request.contentParser())
        searchRequest.source(source)

        val fileName = request.param("fileName")
        val sleep = request.paramAsLong("sleep", 10L)

        val actionRequest = PartialDumpRequest(searchRequest, sleep, fileName)

        return RestChannelConsumer { channel ->
            val listener = RestStatusToXContentListener<PartialDumpResponse>(channel)
            client.execute(PartialDumpAction, actionRequest, listener)
        }
    }
}