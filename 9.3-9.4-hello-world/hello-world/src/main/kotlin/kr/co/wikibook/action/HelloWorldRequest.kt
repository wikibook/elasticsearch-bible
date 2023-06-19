package kr.co.wikibook.action

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput

class HelloWorldRequest : ActionRequest {
    val name: String

    constructor(name: String) : super() {
        this.name = name
    }

    constructor(input: StreamInput) : super(input) {
        this.name = input.readString()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(name)
    }

    override fun validate(): ActionRequestValidationException? {
        return if (name.isEmpty()) {
            val validationException = ActionRequestValidationException()
            validationException.addValidationError("name parameter must not be empty.")
            validationException
        } else {
            null
        }
    }
}