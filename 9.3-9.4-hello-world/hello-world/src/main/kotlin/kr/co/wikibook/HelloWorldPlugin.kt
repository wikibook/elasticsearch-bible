package kr.co.wikibook

import kr.co.wikibook.action.HelloWorldAction
import kr.co.wikibook.action.TransportHelloWorldAction
import kr.co.wikibook.rest.RestHelloWorldAction
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.node.DiscoveryNodes
import org.elasticsearch.common.settings.*
import org.elasticsearch.common.settings.Setting.Property
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import java.util.function.Supplier

class HelloWorldPlugin(private val settings: Settings) : Plugin(), ActionPlugin {
    private val log = LogManager.getLogger(javaClass)

    init {
        log.info("HelloWorldPlugin init")
    }

    override fun getSettings(): MutableList<Setting<*>> {
        return mutableListOf(
            Setting.simpleString("hello.greetings", Property.Dynamic, Property.NodeScope)
        )
    }

    override fun getActions():
            MutableList<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return mutableListOf(
            ActionPlugin.ActionHandler(
                HelloWorldAction, TransportHelloWorldAction::class.java
            )
        )
    }

    override fun getRestHandlers(
        settings: Settings, restController: RestController, clusterSettings: ClusterSettings,
        indexScopedSettings: IndexScopedSettings, settingsFilter: SettingsFilter,
        indexNameExpressionResolver: IndexNameExpressionResolver, nodesInCluster: Supplier<DiscoveryNodes>
    ): MutableList<RestHandler> {
        return mutableListOf(RestHelloWorldAction())
    }
}