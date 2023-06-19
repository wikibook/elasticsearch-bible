package kr.co.wikibook

import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener
import co.elastic.clients.elasticsearch.core.BulkRequest
import co.elastic.clients.elasticsearch.core.BulkResponse
import java.time.LocalDateTime

class BulkIngestListener<Context> : BulkListener<Context> {
    override fun beforeBulk(executionId: Long, request: BulkRequest, contexts: MutableList<Context>) {
        println("[${LocalDateTime.now()}] beforeBulk - executionId : $executionId")
    }

    override fun afterBulk(executionId: Long, request: BulkRequest, contexts: MutableList<Context>, response: BulkResponse) {
        println("[${LocalDateTime.now()}] afterBulk - executionId : $executionId, itemsSize : ${response.items().size}, took : ${response.took()}, errors : ${response.errors()}")
    }

    override fun afterBulk(executionId: Long, request: BulkRequest, contexts: MutableList<Context>, failure: Throwable) {
        System.err.print(failure)
    }
}