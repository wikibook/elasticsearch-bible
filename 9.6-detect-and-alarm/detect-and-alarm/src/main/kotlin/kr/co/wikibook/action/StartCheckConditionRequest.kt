package kr.co.wikibook.action

import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions
import org.elasticsearch.common.io.stream.StreamInput

class StartCheckConditionRequest : ActionRequest {
    val fixedDelaySec: Long

    constructor(fixedDelaySec: Long) : super() {
        this.fixedDelaySec = fixedDelaySec
    }

    constructor(input: StreamInput) : super(input) {
        this.fixedDelaySec = input.readLong()
    }

    override fun validate(): ActionRequestValidationException? {
        val validateException: ActionRequestValidationException? = null
        if (fixedDelaySec <= 0) {
            ValidateActions.addValidationError("fixedDelaySec must be greater than 0", validateException)
        }

        return validateException
    }
}