package kr.co.wikibook

import org.elasticsearch.action.support.ActionFilter
import org.elasticsearch.client.internal.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.NamedWriteableRegistry
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Setting.Property.Dynamic
import org.elasticsearch.common.settings.Setting.Property.NodeScope
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.repositories.RepositoriesService
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.tracing.Tracer
import org.elasticsearch.watcher.ResourceWatcherService
import org.elasticsearch.xcontent.NamedXContentRegistry
import java.util.function.Supplier

class ActionFilterPlugin(private val settings: Settings) : Plugin(), ActionPlugin {
    private val blockActionFilter = BlockActionFilter(settings)

    override fun getSettings(): MutableList<Setting<*>> {
        return mutableListOf(
            Setting.listSetting("blocked.actions", emptyList(), { it }, Dynamic, NodeScope)
        )
    }

    override fun createComponents(client: Client, clusterService: ClusterService, threadPool: ThreadPool,
                                  resourceWatcherService: ResourceWatcherService, scriptService: ScriptService,
                                  xContentRegistry: NamedXContentRegistry, environment: Environment, nodeEnvironment: NodeEnvironment,
                                  namedWriteableRegistry: NamedWriteableRegistry, indexNameExpressionResolver: IndexNameExpressionResolver,
                                  repositoriesServiceSupplier: Supplier<RepositoriesService>, tracer: Tracer): MutableCollection<Any> {

        return mutableListOf(blockActionFilter)
    }

    override fun getActionFilters(): MutableList<ActionFilter> {
        return mutableListOf(blockActionFilter)
    }
}