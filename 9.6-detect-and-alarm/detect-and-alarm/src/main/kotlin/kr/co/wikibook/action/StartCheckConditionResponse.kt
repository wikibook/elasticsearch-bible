package kr.co.wikibook.action

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.StatusToXContentObject
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.xcontent.ToXContent
import org.elasticsearch.xcontent.XContentBuilder

class StartCheckConditionResponse : ActionResponse, StatusToXContentObject {
    constructor() : super()
    constructor(input: StreamInput) : super(input)

    override fun writeTo(out: StreamOutput) {
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("success", true)
        builder.endObject()
        return builder
    }

    override fun status(): RestStatus {
        return RestStatus.OK
    }
}