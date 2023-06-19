package kr.co.wikibook

import kr.co.wikibook.action.HelloWorldAction
import kr.co.wikibook.action.HelloWorldRequest
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.elasticsearch.common.network.NetworkModule
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.test.ESIntegTestCase
import org.elasticsearch.transport.netty4.Netty4Plugin
import org.junit.Test

// if needed, add @ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 3)
class HelloWorldIntegTests : ESIntegTestCase() {
    override fun nodePlugins(): MutableCollection<Class<out Plugin>> {
        return mutableListOf(
            HelloWorldPlugin::class.java,
            Netty4Plugin::class.java
        )
    }

    override fun nodeSettings(nodeOrdinal: Int, otherSettings: Settings?): Settings {
        val defaultSettings = super.nodeSettings(nodeOrdinal, otherSettings)
        return Settings.builder()
            .put(defaultSettings)
            .put(NetworkModule.HTTP_TYPE_SETTING.key, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
            .put("hello.greetings", "my-test-greetings-settings")
            .build()
    }

    override fun addMockHttpTransport(): Boolean {
        return false
    }

    @Test
    fun testHelloAction() {
        val actionRequest = HelloWorldRequest("my-test-name")
        val actionResponse = client().execute(HelloWorldAction, actionRequest).get()

        logger.info("name : {}, greetings : {}", actionResponse.name, actionResponse.greetings)

        assertEquals("my-test-name", actionResponse.name)
        assertEquals("my-test-greetings-settings", actionResponse.greetings)
    }

    @Test
    fun testRestHelloAction() {
        val restClient = getRestClient()

        val request = Request("GET", "/_hello")
        request.addParameter("name", "world2")
        val response = restClient.performRequest(request)

        val statusCode = response.statusLine.statusCode
        assertEquals(200, statusCode)

        val responseString = EntityUtils.toString(response.entity, Charsets.UTF_8)
        logger.info("responseString : {}", responseString)
        val expected = """{"message":"my-test-greetings-settings, world2!"}"""

        assertEquals(expected, responseString)
    }
}