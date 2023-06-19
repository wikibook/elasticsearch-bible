package kr.co.wikibook

import com.carrotsearch.randomizedtesting.annotations.Name
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase

class HelloWorldRestYamlTests(
    @Name("yaml") testCandidate: ClientYamlTestCandidate
) : ESClientYamlSuiteTestCase(testCandidate) {

    companion object {
        @JvmStatic
        @ParametersFactory
        fun parameters(): MutableIterable<Array<Any>> {
            return createParameters()
        }
    }
}