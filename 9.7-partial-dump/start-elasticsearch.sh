#!/bin/sh
export ES_JAVA_OPTS="-Djava.security.manager -Djava.security.policy=file:///path/to/elasticsearch/jdk/lib/security/java.policy"

bin/elasticsearch -d -p pidfile.pid