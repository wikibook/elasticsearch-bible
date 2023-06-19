package kr.co.wikibook.action

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.xcontent.ToXContent
import org.elasticsearch.xcontent.XContentBuilder

class HelloWorldResponse : ActionResponse, ToXContent {
    internal val greetings: String
    internal val name: String

    constructor(greetings: String, name: String) : super() {
        this.greetings = greetings
        this.name = name
    }

    constructor(input: StreamInput) : super(input) {
        this.greetings = input.readString()
        this.name = input.readString()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(greetings)
        out.writeString(name)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params)
            : XContentBuilder {
        builder.startObject()
        builder.field("message", "$greetings, $name!")
        builder.endObject()

        return builder
    }
}