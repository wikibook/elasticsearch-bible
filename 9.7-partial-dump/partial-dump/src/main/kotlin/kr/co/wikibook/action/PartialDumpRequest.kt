package kr.co.wikibook.action

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.IndicesRequest
import org.elasticsearch.action.ValidateActions
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.common.io.stream.StreamInput

class PartialDumpRequest : ActionRequest, IndicesRequest {
    val searchRequest: SearchRequest
    val sleep: Long
    val fileName: String

    constructor(searchRequest: SearchRequest, sleep: Long, fileName: String) : super() {
        this.searchRequest = searchRequest
        this.sleep = sleep
        this.fileName = fileName
    }

    constructor(input: StreamInput) : super(input) {
        this.searchRequest = SearchRequest(input)
        this.sleep = input.readVLong()
        this.fileName = input.readString()
    }

    override fun validate(): ActionRequestValidationException? {
        val validateException: ActionRequestValidationException? = null
        if (sleep < 0) {
            ValidateActions.addValidationError("sleep must be greater than or equal to 0", validateException)
        }

        return validateException
    }

    override fun indices(): Array<String> {
        return searchRequest.indices()
    }

    override fun indicesOptions(): IndicesOptions {
        return searchRequest.indicesOptions()
    }
}