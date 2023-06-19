package kr.co.wikibook.action

import kr.co.wikibook.service.DumpService
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.IndicesRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchScrollRequest
import org.elasticsearch.action.search.TransportSearchAction
import org.elasticsearch.action.search.TransportSearchScrollAction
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.PlainActionFuture
import org.elasticsearch.action.support.TransportAction
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.core.TimeValue
import org.elasticsearch.search.SearchHits
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import org.elasticsearch.xcontent.XContentType

class TransportPartialDumpAction @Inject constructor(
    actionFilters: ActionFilters,
    transportService: TransportService,
    private val transportSearchAction: TransportSearchAction,
    private val transportSearchScrollAction: TransportSearchScrollAction,
    private val dumpService: DumpService
) : TransportAction<PartialDumpRequest, PartialDumpResponse>(
    NAME, actionFilters, transportService.taskManager
) {
    private val log = LogManager.getLogger(javaClass)

    override fun doExecute(task: Task, request: PartialDumpRequest, listener: ActionListener<PartialDumpResponse>) {
        val searchRequest = request.searchRequest

        val searchTask = taskManager.register("transport", transportSearchAction.actionName, request.searchRequest)

        // 첫 번째 검색
        searchRequest.scroll(TimeValue.timeValueMinutes(1L))
        transportSearchAction.execute(searchTask, searchRequest, object : ActionListener<SearchResponse> {
            override fun onResponse(response: SearchResponse) {
                try {
                    doScrollSearch(searchTask, request, response, listener)
                } finally {
                    taskManager.unregister(task)
                }
            }

            override fun onFailure(e: Exception) {
                try {
                    listener.onFailure(e)
                } finally {
                    taskManager.unregister(task)
                }
            }
        })
    }

    private fun doScrollSearch(task: Task, request: PartialDumpRequest, response: SearchResponse, listener: ActionListener<PartialDumpResponse>) {
        var scrollId: String? = response.scrollId
        var hitsSize = response.hits.hits.size
        var writeCount = doWrite(response.hits, request.fileName, false)
        Thread.sleep(request.sleep)

        // scroll 검색
        while (hitsSize != 0 && !scrollId.isNullOrBlank()) {
            val scrollRequest = SearchScrollRequest(scrollId).scroll(TimeValue.timeValueMinutes(1L))

            val scrollFuture = PlainActionFuture.newFuture<SearchResponse>()
            transportSearchScrollAction.execute(task, scrollRequest, object : ActionListener<SearchResponse> {
                override fun onResponse(response: SearchResponse) {
                    scrollFuture.onResponse(response)
                }

                override fun onFailure(e: Exception) {
                    scrollFuture.onFailure(e)
                }
            })
            val scrollResult = scrollFuture.actionGet()

            scrollId = scrollResult.scrollId
            hitsSize = scrollResult.hits.hits.size
            writeCount += doWrite(scrollResult.hits, request.fileName, true)
            Thread.sleep(request.sleep)
        }

        listener.onResponse(PartialDumpResponse(writeCount))
    }

    private fun doWrite(hits: SearchHits, fileName: String, append: Boolean): Long {
        val lines = hits.hits
            .map { hit ->
                XContentHelper.convertToJson(hit.sourceRef, true, false, XContentType.JSON)
            }

        return dumpService.writeLines(fileName, lines, append)
    }
}