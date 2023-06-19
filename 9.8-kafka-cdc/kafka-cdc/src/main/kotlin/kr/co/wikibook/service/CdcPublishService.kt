package kr.co.wikibook.service

import kr.co.wikibook.common.toMap
import kr.co.wikibook.service.CdcPublishService.CdcPublisherType.KAFKA
import kr.co.wikibook.service.CdcPublishService.CdcPublisherType.LOG
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.component.AbstractLifecycleComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.engine.Engine

class CdcPublishService @Inject constructor(val settings: Settings) : AbstractLifecycleComponent() {
    val publisher: CdcPublisher
    private val cdcType = CdcPublisherType.valueOf(settings.get("cdc.type", "LOG"))

    init {
        this.publisher = when (cdcType) {
            LOG -> LogCdcPublisher()
            KAFKA -> KafkaCdcPublisher(settings)
        }
    }

    fun buildCdcMessage(indexName: String, shard: Int, index: Engine.Index): CdcMessage {
        val afterStr = BytesReference.toBytes(index.source()).toString(Charsets.UTF_8)
        val after = afterStr.toMap()

        return CdcMessage(
            operation = index.operationType().name,
            index = indexName,
            shard = shard,
            id = index.id(),
            routing = index.routing(),
            before = null,
            after = after
        )
    }

    fun buildCdcMessage(indexName: String, shard: Int, delete: Engine.Delete): CdcMessage {
        return CdcMessage(
            operation = delete.operationType().name,
            index = indexName,
            shard = shard,
            id = delete.id(),
            routing = null,
            before = null,
            after = null
        )
    }

    override fun doStart() {
    }

    override fun doStop() {
    }

    override fun doClose() {
        publisher.close()
    }

    enum class CdcPublisherType {
        LOG, KAFKA
    }
}