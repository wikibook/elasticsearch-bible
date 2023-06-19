package kr.co.wikibook

import org.apache.http.util.EntityUtils
import org.apache.logging.log4j.LogManager
import org.elasticsearch.client.Request
import org.elasticsearch.test.rest.ESRestTestCase

class HelloWorldRestJavaTests : ESRestTestCase() {
    fun testRestAction() {
        val restClient = client()

        val request = Request("GET", "/_hello")
        request.addParameter("name", "world2")
        val response = restClient.performRequest(request)

        val statusCode = response.statusLine.statusCode
        assertEquals(200, statusCode)

        val responseString = EntityUtils.toString(response.entity, Charsets.UTF_8)
        logger.info("responseString : {}", responseString)
        val expected = """{"message":"hello, world2!"}"""

        assertEquals(expected, responseString)
    }
}