package kr.co.wikibook.action

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.StatusToXContentObject
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.xcontent.ToXContent
import org.elasticsearch.xcontent.XContentBuilder

class PartialDumpResponse : ActionResponse, StatusToXContentObject {
    private val writeCount: Long

    constructor(writeCount: Long) : super() {
        this.writeCount = writeCount
    }

    constructor(input: StreamInput) : super(input) {
        this.writeCount = input.readVLong()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeVLong(writeCount)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("writeCount", writeCount)
        builder.endObject()

        return builder
    }

    override fun status(): RestStatus {
        return RestStatus.OK
    }
}