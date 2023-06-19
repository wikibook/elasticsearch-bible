package kr.co.wikibook

import kr.co.wikibook.task.CheckConditionPersistentTaskParams.Companion.TASK_NAME
import kr.co.wikibook.action.RestStartCheckConditionAction
import kr.co.wikibook.action.StartCheckConditionAction
import kr.co.wikibook.action.TransportStartCheckConditionAction
import kr.co.wikibook.task.CheckConditionPersistentTaskExecutor
import kr.co.wikibook.task.CheckConditionPersistentTaskParams
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.client.internal.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.node.DiscoveryNodes
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.NamedWriteableRegistry
import org.elasticsearch.common.settings.*
import org.elasticsearch.common.settings.Setting.Property.Dynamic
import org.elasticsearch.common.settings.Setting.Property.NodeScope
import org.elasticsearch.persistent.PersistentTaskParams
import org.elasticsearch.persistent.PersistentTasksExecutor
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.PersistentTaskPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.xcontent.NamedXContentRegistry
import org.elasticsearch.xcontent.ParseField
import java.util.function.Supplier

class DetectAndAlarmPlugin(private val settings: Settings) : Plugin(), ActionPlugin, PersistentTaskPlugin {
    override fun getSettings(): MutableList<Setting<*>> {
        return mutableListOf(
            Setting.simpleString("detect.alarm.url", Dynamic, NodeScope),
            Setting.simpleString("detect.alarm.email", Dynamic, NodeScope),
            Setting.longSetting("detect.condition.timeoutSec", 30L, 0L, Dynamic, NodeScope),
        )
    }

    override fun getActions(): MutableList<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return mutableListOf(
            ActionPlugin.ActionHandler(StartCheckConditionAction, TransportStartCheckConditionAction::class.java)
        )
    }

    override fun getRestHandlers(
        settings: Settings,
        restController: RestController,
        clusterSettings: ClusterSettings,
        indexScopedSettings: IndexScopedSettings,
        settingsFilter: SettingsFilter,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        nodesInCluster: Supplier<DiscoveryNodes>
    ): MutableList<RestHandler> {
        return mutableListOf(
            RestStartCheckConditionAction()
        )
    }

    override fun getPersistentTasksExecutor(
        clusterService: ClusterService,
        threadPool: ThreadPool,
        client: Client,
        settingsModule: SettingsModule,
        expressionResolver: IndexNameExpressionResolver
    ): MutableList<PersistentTasksExecutor<*>> {
        return mutableListOf(
            CheckConditionPersistentTaskExecutor(ThreadPool.Names.SEARCH, settings, client)
        )
    }

    override fun getNamedWriteables(): MutableList<NamedWriteableRegistry.Entry> {
        return mutableListOf(
            NamedWriteableRegistry.Entry(PersistentTaskParams::class.java,TASK_NAME,::CheckConditionPersistentTaskParams)
        )
    }

    override fun getNamedXContent(): MutableList<NamedXContentRegistry.Entry> {
        return mutableListOf(
            NamedXContentRegistry.Entry(PersistentTaskParams::class.java, ParseField(TASK_NAME), CheckConditionPersistentTaskParams::fromXContent)
        )
    }
}