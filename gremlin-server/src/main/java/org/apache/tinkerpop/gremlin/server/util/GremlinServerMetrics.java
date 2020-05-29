/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.util;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.util.MetricManager;

/**
 * Singleton that contains and configures Gremlin Server's {@code MetricRegistry}. Borrowed from Titan's approach to
 * managing Metrics.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerMetrics {

    private static MetricRegistry getRegistry() {
        return MetricManager.INSTANCE.getRegistry();
    }

    /**
     * Registers metrics from a {@link GremlinScriptEngine}. At this point, this only works for the
     * {@link GremlinGroovyScriptEngine} as it is the only one that collects metrics at this point. As the
     * {@link GremlinScriptEngine} implementations achieve greater parity these metrics will get expanded.
     */
    public static void registerGremlinScriptEngineMetrics(final GremlinScriptEngine engine, final String... prefix) {
        // only register if metrics aren't already registered. typically only happens in testing where two gremlin
        // server instances are running in the same jvm. they will share the same metrics if that is the case since
        // the MetricsManager is static
        if (engine instanceof GremlinGroovyScriptEngine && getRegistry().getNames().stream().noneMatch(n -> n.endsWith("long-run-compilation-count"))) {
            final GremlinGroovyScriptEngine gremlinGroovyScriptEngine = (GremlinGroovyScriptEngine) engine;
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "long-run-compilation-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheLongRunCompilationCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "estimated-size")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheEstimatedSize);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "average-load-penalty")),
                    (Gauge<Double>) gremlinGroovyScriptEngine::getClassCacheAverageLoadPenalty);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "eviction-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheEvictionCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "eviction-weight")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheEvictionWeight);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "hit-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheHitCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "hit-rate")),
                    (Gauge<Double>) gremlinGroovyScriptEngine::getClassCacheHitRate);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "load-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheLoadCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "load-failure-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheLoadFailureCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "load-failure-rate")),
                    (Gauge<Double>) gremlinGroovyScriptEngine::getClassCacheLoadFailureRate);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "load-success-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheLoadSuccessCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "miss-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheMissCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "miss-rate")),
                    (Gauge<Double>) gremlinGroovyScriptEngine::getClassCacheMissRate);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "request-count")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheRequestCount);
            getRegistry().register(
                    MetricRegistry.name(GremlinServer.class, ArrayUtils.add(prefix, "total-load-time")),
                    (Gauge<Long>) gremlinGroovyScriptEngine::getClassCacheTotalLoadTime);
        }
    }
}
