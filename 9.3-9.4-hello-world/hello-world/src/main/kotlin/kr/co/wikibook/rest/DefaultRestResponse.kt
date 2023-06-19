package kr.co.wikibook.rest

import org.elasticsearch.common.Strings
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.RestStatus.INTERNAL_SERVER_ERROR
import org.elasticsearch.rest.RestStatus.OK
import org.elasticsearch.xcontent.ToXContent

class DefaultRestResponse private constructor(
    private val restStatus: RestStatus,
    private val content: String
) : RestResponse(restStatus, content) {
    override fun contentType(): String = "application/json; charset=UTF-8"

    override fun status(): RestStatus = restStatus

    override fun content(): BytesReference = BytesArray(content)

    companion object {
        fun success(toXContent: ToXContent, channel: RestChannel, restStatus: RestStatus = OK): DefaultRestResponse {
            val xContentBuilder = channel.newBuilder()
            toXContent.toXContent(xContentBuilder, channel.request())
            return DefaultRestResponse(restStatus, Strings.toString(xContentBuilder))
        }

        fun error(e: Throwable, restStatus: RestStatus = INTERNAL_SERVER_ERROR): DefaultRestResponse {
            return DefaultRestResponse(restStatus, buildJsonContent(e))
        }

        private fun buildJsonContent(e: Throwable) = """{"errorMessage":"$e"}"""
    }
}