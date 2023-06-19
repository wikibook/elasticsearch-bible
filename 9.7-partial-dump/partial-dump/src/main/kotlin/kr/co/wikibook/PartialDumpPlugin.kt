package kr.co.wikibook

import kr.co.wikibook.action.PartialDumpAction
import kr.co.wikibook.action.TransportPartialDumpAction
import kr.co.wikibook.rest.RestPartialDumpAction
import kr.co.wikibook.service.DumpService
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
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.ActionPlugin.ActionHandler
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.repositories.RepositoriesService
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.tracing.Tracer
import org.elasticsearch.watcher.ResourceWatcherService
import org.elasticsearch.xcontent.NamedXContentRegistry
import java.util.function.Supplier

class PartialDumpPlugin(private val settings: Settings) : Plugin(), ActionPlugin {
    override fun getSettings(): MutableList<Setting<*>> {
        return mutableListOf(
            Setting.simpleString("dump.type", Dynamic, NodeScope),

            Setting.simpleString("dump.fs.path", Dynamic, NodeScope),

            Setting.listSetting("dump.hdfs.configs", emptyList(), { it }, Dynamic, NodeScope),
            Setting.simpleString("dump.hdfs.defaultFs", Dynamic, NodeScope),
            Setting.simpleString("dump.hdfs.userName", Dynamic, NodeScope)
        )
    }

    override fun createComponents(client: Client, clusterService: ClusterService, threadPool: ThreadPool,
                                  resourceWatcherService: ResourceWatcherService, scriptService: ScriptService,
                                  xContentRegistry: NamedXContentRegistry, environment: Environment, nodeEnvironment: NodeEnvironment,
                                  namedWriteableRegistry: NamedWriteableRegistry, indexNameExpressionResolver: IndexNameExpressionResolver,
                                  repositoriesServiceSupplier: Supplier<RepositoriesService>, tracer: Tracer): MutableCollection<Any> {

        return mutableListOf(DumpService(settings))
    }

    override fun getActions(): MutableList<ActionHandler<out ActionRequest, out ActionResponse>> {
        return mutableListOf(
            ActionHandler(PartialDumpAction, TransportPartialDumpAction::class.java)
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
            RestPartialDumpAction()
        )
    }
}