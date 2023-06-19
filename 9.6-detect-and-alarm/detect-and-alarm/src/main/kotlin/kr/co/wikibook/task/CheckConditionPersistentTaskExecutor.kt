package kr.co.wikibook.task

import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.result.Result
import kr.co.wikibook.task.CheckConditionPersistentTaskParams.Companion.TASK_NAME
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.search.SearchAction
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.internal.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.RangeQueryBuilder
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.persistent.AllocatedPersistentTask
import org.elasticsearch.persistent.PersistentTaskState
import org.elasticsearch.persistent.PersistentTasksCustomMetadata
import org.elasticsearch.persistent.PersistentTasksExecutor
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.filter.Filter
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.tasks.TaskId
import java.security.AccessController
import java.security.PrivilegedExceptionAction
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class CheckConditionPersistentTaskExecutor(
    executor: String,
    settings: Settings,
    private val client: Client
) : PersistentTasksExecutor<CheckConditionPersistentTaskParams>(TASK_NAME, executor) {
    private val log = LogManager.getLogger(javaClass)
    private val scheduledExecutorService = Executors.newScheduledThreadPool(10)

    private val alarmUrl = settings.get("detect.alarm.url")
    private val alarmEmail = settings.get("detect.alarm.email")
    private val timeoutSec = settings.getAsLong("detect.condition.timeoutSec", 30L)

    override fun createTask(
        id: Long,
        type: String,
        action: String,
        parentTaskId: TaskId,
        taskInProgress: PersistentTasksCustomMetadata.PersistentTask<CheckConditionPersistentTaskParams>,
        headers: MutableMap<String, String>
    ): AllocatedPersistentTask {
        headers["alarmUrl"] = alarmUrl
        headers["alarmEmail"] = alarmEmail
        headers["timeoutSec"] = timeoutSec.toString()

        return CheckConditionPersistentTask(id, type, action, getDescription(taskInProgress), parentTaskId, headers)
    }

    override fun nodeOperation(task: AllocatedPersistentTask, params: CheckConditionPersistentTaskParams, state: PersistentTaskState?) {
        task.headers()["fixedDelaySec"] = params.fixedDelaySec.toString()

        // 반복 수행할 작업을 scheduled executor에 등록한다.
        val future = scheduledExecutorService.scheduleWithFixedDelay(
            detectAndAlarmRunnable(params), 1L, params.fixedDelaySec, TimeUnit.SECONDS)
        (task as? CheckConditionPersistentTask)?.future = future
    }

    private fun detectAndAlarmRunnable(params: CheckConditionPersistentTaskParams): Runnable {
        val queryBuilder = RangeQueryBuilder("@timestamp")
            .gt("now-30m")

        val successFilter = TermQueryBuilder("success", true)
        val aggregationBuilder = AggregationBuilders.filter("success-value", successFilter)

        val searchSourceBuilder = SearchSourceBuilder()
            .from(0)
            .size(0)
            .query(queryBuilder)
            .aggregation(aggregationBuilder)

        val searchRequest = SearchRequest("alarm-test-index")
            .source(searchSourceBuilder)

        val runnable = Runnable {
            val searchResponse = client.execute(SearchAction.INSTANCE, searchRequest).actionGet(timeoutSec, TimeUnit.SECONDS)
            val filter = searchResponse.aggregations?.asMap?.get("success-value") as? Filter
            val hitCount = searchResponse.hits.totalHits!!.value
            val successDocCount = filter?.docCount

            if (successDocCount == null || (successDocCount.toFloat() / hitCount) < 0.5) {
                AccessController.doPrivileged(PrivilegedExceptionAction {
                    doAlarm(searchResponse)
                })
            }
        }

        return runnable
    }

    private fun doAlarm(searchResponse: SearchResponse) {
        log.info("alarmUrl : {}, alarmEmail : {}, searchResponse : {}", alarmUrl, alarmEmail, searchResponse.toString())
        val url = "$alarmUrl?email=$alarmEmail&message=$searchResponse"
        val (_, _, result) = url.httpGet().responseString()

        when (result) {
            is Result.Success -> {
                log.info("alarm response : {}", result.get())
            }

            is Result.Failure -> {
                val e = result.error.exception
                log.error(e.toString(), e)
            }
        }
    }
}