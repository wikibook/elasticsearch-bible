package kr.co.wikibook

import kr.co.wikibook.action.HelloWorldAction
import kr.co.wikibook.action.HelloWorldRequest
import org.apache.logging.log4j.LogManager
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.test.ESSingleNodeTestCase
import org.elasticsearch.test.ESTestCase
import org.junit.Test

class HelloWorldSingleNodeTests : ESSingleNodeTestCase() {
    override fun getPlugins(): MutableCollection<Class<out Plugin>> {
        return mutableListOf(HelloWorldPlugin::class.java)
    }

    override fun nodeSettings(): Settings {
        return Settings.builder()
            .put("hello.greetings", "my-test-greetings-settings")
            .build()
    }

    @Test
    fun testHelloAction() {
        val actionRequest = HelloWorldRequest("my-test-name")
        val actionResponse = client().execute(HelloWorldAction, actionRequest).get()

        logger.info("name : {}, greetings : {}",
            actionResponse.name, actionResponse.greetings)

        assertEquals("my-test-name", actionResponse.name)
        assertEquals("my-test-greetings-settings", actionResponse.greetings)
    }

    // there is no @Test annotation but it works because of the 'test' method name prefix
    fun testHelloActionWithRandomStrings() {
        val maxArraySize = 10
        val stringSize = 10
        val allowNull = false
        val allowEmpty = true

        val randomStrArray = generateRandomStringArray(maxArraySize, stringSize,allowNull, allowEmpty)

        for (name in randomStrArray) {
            val actionRequest = HelloWorldRequest(name)
            val actionResponse = client().execute(HelloWorldAction, actionRequest).get()

            logger.info("name : {}, greetings : {}",
                actionResponse.name, actionResponse.greetings)

            assertEquals(name, actionResponse.name)
            assertEquals("my-test-greetings-settings", actionResponse.greetings)
        }
    }
}