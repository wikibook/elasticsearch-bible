package kr.co.wikibook

import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.support.ActionFilter
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.TermsQueryBuilder

class BlockActionFilter @Inject constructor(settings: Settings) : ActionFilter.Simple() {
    private val log = LogManager.getLogger(javaClass)
    private val blockedActions = settings.getAsList("blocked.actions", listOf()).toSet()

    override fun apply(action: String?, request: ActionRequest, listener: ActionListener<*>): Boolean {
        if (blockedActions.contains(action)) {
            log.info("block action filtered")
            listener.onFailure(SecurityException("unauthorized action"))
            return false
        }

        if (request is SearchRequest) {
            // request의 내용을 파악하고 수행 여부를 결정할 수 있으며
            var totalAggsCount = 0
            val aggregations = request.source().aggregations()

            aggregations?.aggregatorFactories?.forEach {
                totalAggsCount++

                val subAggregations = it.subAggregations
                val tooDeep = subAggregations.any { subAggregation -> subAggregation.subAggregations.isNotEmpty() }
                if (tooDeep) {
                    listener.onFailure(IllegalArgumentException("too deep aggregations"))
                    return false
                }

                totalAggsCount += subAggregations.size
            }

            if (totalAggsCount > 5) {
                listener.onFailure(IllegalArgumentException("too many aggregations"))
                return false
            }

            // request를 변형해서 실행하는 것도 가능
            val queryBuilder = request.source().query()
            if (queryBuilder is TermsQueryBuilder) {
                val terms = queryBuilder.values()
                if (terms.size > 5) {
                    val newQueryBuilder = QueryBuilders.termsQuery(queryBuilder.fieldName(), terms.take(5))
                    request.source().query(newQueryBuilder)
                }
            }
        }

        return true
    }

    override fun order(): Int = Integer.MIN_VALUE
}