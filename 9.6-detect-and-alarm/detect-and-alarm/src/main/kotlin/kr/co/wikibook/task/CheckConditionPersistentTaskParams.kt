package kr.co.wikibook.task

import org.elasticsearch.Version
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.persistent.PersistentTaskParams
import org.elasticsearch.xcontent.ParseField
import org.elasticsearch.xcontent.ToXContent
import org.elasticsearch.xcontent.XContentBuilder
import org.elasticsearch.xcontent.XContentParser

class CheckConditionPersistentTaskParams : PersistentTaskParams {
    val fixedDelaySec: Long

    constructor(fixedDelaySec: Long) {
        this.fixedDelaySec = fixedDelaySec
    }

    constructor(input: StreamInput) {
        this.fixedDelaySec = input.readLong()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeLong(fixedDelaySec)
    }

    override fun getWriteableName(): String = TASK_NAME

    override fun getMinimalSupportedVersion(): Version = Version.V_7_0_0

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field(FIXED_DELAY_SEC_FIELD_NAME, fixedDelaySec)
        builder.endObject()
        return builder
    }

    companion object {
        const val TASK_NAME = "detect-and-alarm-task"
        const val FIXED_DELAY_SEC_FIELD_NAME = "fixedDelaySec"
        private val FIXED_DELAY_SEC_FIELD = ParseField(FIXED_DELAY_SEC_FIELD_NAME)

        fun fromXContent(parser: XContentParser): CheckConditionPersistentTaskParams {
            var fixedDelaySec: Long? = null
            var currentFieldName: String? = null
            var currentToken = parser.nextToken()

            while (currentToken != XContentParser.Token.END_OBJECT) {
                if (currentToken == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName()
                } else if (FIXED_DELAY_SEC_FIELD.match(currentFieldName, parser.deprecationHandler)) {
                    fixedDelaySec = parser.longValue()
                }

                currentToken = parser.nextToken()
            }

            return CheckConditionPersistentTaskParams(fixedDelaySec!!)
        }
    }
}