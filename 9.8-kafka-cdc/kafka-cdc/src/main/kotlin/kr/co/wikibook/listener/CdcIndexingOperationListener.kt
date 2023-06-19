package kr.co.wikibook.listener

import kr.co.wikibook.service.CdcPublishService
import org.elasticsearch.index.engine.Engine
import org.elasticsearch.index.shard.IndexingOperationListener
import org.elasticsearch.index.shard.ShardId

class CdcIndexingOperationListener(private val cdcPublishService: CdcPublishService) : IndexingOperationListener {
    override fun preIndex(shardId: ShardId, operation: Engine.Index): Engine.Index {
        return super.preIndex(shardId, operation)
    }

    override fun postIndex(shardId: ShardId, index: Engine.Index, result: Engine.IndexResult) {
        if (validatePublishCondition(shardId, index, result)) {
            cdcPublishService.publisher
                .publish(cdcPublishService.buildCdcMessage(shardId.indexName, shardId.id, index))
        }
    }

    override fun postIndex(shardId: ShardId, index: Engine.Index, ex: Exception) {
        // 엔진 레벨에서 에러 발생시
        super.postIndex(shardId, index, ex)
    }

    override fun preDelete(shardId: ShardId, delete: Engine.Delete): Engine.Delete {
        return super.preDelete(shardId, delete)
    }

    override fun postDelete(shardId: ShardId, delete: Engine.Delete, result: Engine.DeleteResult) {
        if (validatePublishCondition(shardId, delete, result)) {
            cdcPublishService.publisher
                .publish(cdcPublishService.buildCdcMessage(shardId.indexName, shardId.id, delete))
        }
    }

    override fun postDelete(shardId: ShardId, delete: Engine.Delete, ex: Exception) {
        // 엔진 레벨에서 에러 발생시
        super.postDelete(shardId, delete, ex)
    }

    private fun validatePublishCondition(shardId: ShardId, operation: Engine.Operation, result: Engine.Result): Boolean {
        if (result.failure != null ||
            operation.origin() != Engine.Operation.Origin.PRIMARY ||
            shardId.indexName.startsWith(".")) {
            return false
        }

        return true
    }
}