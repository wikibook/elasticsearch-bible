package kr.co.wikibook

import kr.co.wikibook.listener.CdcIndexingOperationListener
import kr.co.wikibook.service.CdcPublishService
import org.elasticsearch.client.internal.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.NamedWriteableRegistry
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Setting.Property.*
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.index.IndexModule
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.repositories.RepositoriesService
import org.elasticsearch.script.ScriptService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.tracing.Tracer
import org.elasticsearch.watcher.ResourceWatcherService
import org.elasticsearch.xcontent.NamedXContentRegistry
import java.util.function.Supplier

class KafkaCdcPlugin(private val settings: Settings) : Plugin(), ActionPlugin {
    private lateinit var cdcPublishService: CdcPublishService

    override fun getSettings(): MutableList<Setting<*>> {
        return mutableListOf(
            Setting.simpleString("cdc.type", Dynamic, NodeScope),
            Setting.simpleString("cdc.kafka.brokers", Dynamic, NodeScope),
            Setting.simpleString("cdc.kafka.topicSuffix", Dynamic, NodeScope),
            Setting.prefixKeySetting("cdc.kafka.settings.") {
                Setting.simpleString(it, Final, NodeScope)
            }
        )
    }

    override fun createComponents(
        client: Client, clusterService: ClusterService, threadPool: ThreadPool, resourceWatcherService: ResourceWatcherService,
        scriptService: ScriptService, xContentRegistry: NamedXContentRegistry, environment: Environment, nodeEnvironment: NodeEnvironment,
        namedWriteableRegistry: NamedWriteableRegistry, indexNameExpressionResolver: IndexNameExpressionResolver,
        repositoriesServiceSupplier: Supplier<RepositoriesService>, tracer: Tracer): MutableCollection<Any> {

        cdcPublishService = CdcPublishService(settings)

        return mutableListOf(cdcPublishService)
    }

    override fun onIndexModule(indexModule: IndexModule) {
        indexModule.addIndexOperationListener(CdcIndexingOperationListener(cdcPublishService))
        super.onIndexModule(indexModule)
    }
}