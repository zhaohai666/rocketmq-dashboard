/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.dashboard.service.metrics;

import org.apache.rocketmq.dashboard.architecture.MetadataProvider;
import org.apache.rocketmq.dashboard.service.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Default implementation of {@link MetricsAggregationService}.
 *
 * <p>Aggregates metrics from two sources:</p>
 * <ol>
 *   <li><b>Prometheus</b> (via {@link PrometheusMetricsQueryClient}) - time-series
 *       metrics queried using PromQL, parsed from OpenMetrics format</li>
 *   <li><b>Native MetadataProvider</b> (via {@link MetricsService}) - operational
 *       data from the RocketMQ cluster admin API</li>
 * </ol>
 *
 * <p>When Prometheus is not configured, the service gracefully degrades to
 * returning only native metrics. When MetadataProvider does not support metrics,
 * only Prometheus data is returned.</p>
 */
@Service
public class MetricsAggregationServiceImpl implements MetricsAggregationService {

    private static final Logger log = LoggerFactory.getLogger(MetricsAggregationServiceImpl.class);

    @Resource
    private PrometheusMetricsQueryClient prometheusClient;

    @Resource
    private MetricsService metricsService;

    @Resource
    private MetadataProvider metadataProvider;

    // ==================== Dashboard Metrics ====================

    @Override
    public Map<String, Object> getDashboardMetrics() {
        Map<String, Object> dashboard = new LinkedHashMap<>();
        dashboard.put("timestamp", System.currentTimeMillis());

        // 1. Prometheus cluster metrics
        Map<String, Object> prometheusCluster = safeGetPrometheusCluster();
        dashboard.put("prometheusClusterMetrics", prometheusCluster);

        // 2. Native cluster metrics from MetadataProvider
        Map<String, Object> nativeCluster = safeGetNativeClusterMetrics();
        dashboard.put("nativeClusterMetrics", nativeCluster);

        // 3. Merged/aggregated view
        Map<String, Object> merged = new LinkedHashMap<>();
        mergeMetrics(merged, nativeCluster);
        mergeMetrics(merged, extractMetricsFromPrometheusResponse(prometheusCluster));
        dashboard.put("metrics", merged);

        // 4. Data source health summary
        dashboard.put("dataSourcesHealth", getDataSourcesHealth());

        return dashboard;
    }

    // ==================== Broker Metrics ====================

    @Override
    public Map<String, Object> getAggregatedBrokerMetrics(String brokerName) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("brokerName", brokerName);
        result.put("timestamp", System.currentTimeMillis());

        // Prometheus broker metrics
        Map<String, Object> prometheusBroker = safeGetPrometheusBroker(brokerName);
        result.put("prometheusMetrics", prometheusBroker);

        // Native broker metrics
        Map<String, Object> nativeBroker = safeGetNativeBrokerMetrics(brokerName);
        result.put("nativeMetrics", nativeBroker);

        // Merged view
        Map<String, Object> merged = new LinkedHashMap<>();
        mergeMetrics(merged, nativeBroker);
        mergeMetrics(merged, extractMetricsFromPrometheusResponse(prometheusBroker));
        result.put("metrics", merged);

        return result;
    }

    // ==================== Topic Metrics ====================

    @Override
    public Map<String, Object> getAggregatedTopicMetrics(String topicName) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("topicName", topicName);
        result.put("timestamp", System.currentTimeMillis());

        // Prometheus topic metrics
        Map<String, Object> prometheusTopic = safeGetPrometheusTopic(topicName);
        result.put("prometheusMetrics", prometheusTopic);

        // Native topic metrics
        Map<String, Object> nativeTopic = safeGetNativeTopicMetrics(topicName);
        result.put("nativeMetrics", nativeTopic);

        // Merged view
        Map<String, Object> merged = new LinkedHashMap<>();
        mergeMetrics(merged, nativeTopic);
        mergeMetrics(merged, extractMetricsFromPrometheusResponse(prometheusTopic));
        result.put("metrics", merged);

        return result;
    }

    // ==================== Consumer Group Metrics ====================

    @Override
    public Map<String, Object> getAggregatedConsumerGroupMetrics(String groupName) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupName", groupName);
        result.put("timestamp", System.currentTimeMillis());

        // Prometheus consumer metrics
        Map<String, Object> prometheusConsumer = safeGetPrometheusConsumer(groupName);
        result.put("prometheusMetrics", prometheusConsumer);

        // Native consumer metrics
        Map<String, Object> nativeConsumer = safeGetNativeConsumerMetrics(groupName);
        result.put("nativeMetrics", nativeConsumer);

        // Merged view
        Map<String, Object> merged = new LinkedHashMap<>();
        mergeMetrics(merged, nativeConsumer);
        mergeMetrics(merged, extractMetricsFromPrometheusResponse(prometheusConsumer));
        result.put("metrics", merged);

        return result;
    }

    // ==================== System Metrics ====================

    @Override
    public Map<String, Object> getAggregatedSystemMetrics() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("timestamp", System.currentTimeMillis());

        // Prometheus system metrics
        Map<String, Object> prometheusSystem = safeGetPrometheusSystem();
        result.put("prometheusMetrics", prometheusSystem);

        // Native system metrics
        Map<String, Object> nativeSystem = safeGetNativeSystemMetrics();
        result.put("nativeMetrics", nativeSystem);

        // Merged view
        Map<String, Object> merged = new LinkedHashMap<>();
        mergeMetrics(merged, nativeSystem);
        mergeMetrics(merged, extractMetricsFromPrometheusResponse(prometheusSystem));
        result.put("metrics", merged);

        return result;
    }

    // ==================== Data Source Health ====================

    @Override
    public Map<String, Object> getDataSourcesHealth() {
        Map<String, Object> health = new LinkedHashMap<>();

        // Prometheus health
        Map<String, Object> prometheusHealth = new LinkedHashMap<>();
        boolean prometheusAvailable = prometheusClient.isDatasourceAvailable(null);
        prometheusHealth.put("available", prometheusAvailable);
        prometheusHealth.put("type", "prometheus");
        if (prometheusAvailable) {
            prometheusHealth.put("status", "connected");
        } else {
            prometheusHealth.put("status", "unavailable");
            prometheusHealth.put("message", "Prometheus datasource not configured or unreachable");
        }
        health.put("prometheus", prometheusHealth);

        // Native MetadataProvider health
        Map<String, Object> nativeHealth = new LinkedHashMap<>();
        try {
            Map<String, Object> summary = metricsService.getMetricsSummary();
            boolean metricsSupported = Boolean.TRUE.equals(summary.get("metrics_supported"));
            nativeHealth.put("available", metricsSupported);
            nativeHealth.put("type", "metadataProvider");
            nativeHealth.put("status", metricsSupported ? "connected" : "unsupported");
            nativeHealth.put("capabilities", summary);
        } catch (Exception e) {
            nativeHealth.put("available", false);
            nativeHealth.put("type", "metadataProvider");
            nativeHealth.put("status", "error");
            nativeHealth.put("message", e.getMessage());
        }
        health.put("native", nativeHealth);

        return health;
    }

    // ==================== PromQL Proxy ====================

    @Override
    public Map<String, Object> executePromQL(String promQL, String datasourceId) {
        return prometheusClient.queryInstant(promQL, datasourceId);
    }

    @Override
    public Map<String, Object> executePromQLRange(String promQL, String start, String end, String step, String datasourceId) {
        return prometheusClient.queryRange(promQL, start, end, step, datasourceId);
    }

    // ==================== Prometheus Query Helpers ====================

    private Map<String, Object> safeGetPrometheusCluster() {
        try {
            return prometheusClient.clusterMetrics(null);
        } catch (Exception e) {
            log.debug("Prometheus cluster metrics unavailable: {}", e.getMessage());
            return unavailableResult("prometheus");
        }
    }

    private Map<String, Object> safeGetPrometheusBroker(String brokerName) {
        try {
            return prometheusClient.brokerMetrics(brokerName, null);
        } catch (Exception e) {
            log.debug("Prometheus broker metrics unavailable for {}: {}", brokerName, e.getMessage());
            return unavailableResult("prometheus");
        }
    }

    private Map<String, Object> safeGetPrometheusTopic(String topicName) {
        try {
            return prometheusClient.topicMetrics(topicName, null);
        } catch (Exception e) {
            log.debug("Prometheus topic metrics unavailable for {}: {}", topicName, e.getMessage());
            return unavailableResult("prometheus");
        }
    }

    private Map<String, Object> safeGetPrometheusConsumer(String groupName) {
        try {
            return prometheusClient.consumerGroupMetrics(groupName, null);
        } catch (Exception e) {
            log.debug("Prometheus consumer metrics unavailable for {}: {}", groupName, e.getMessage());
            return unavailableResult("prometheus");
        }
    }

    private Map<String, Object> safeGetPrometheusSystem() {
        try {
            return prometheusClient.systemMetrics(null);
        } catch (Exception e) {
            log.debug("Prometheus system metrics unavailable: {}", e.getMessage());
            return unavailableResult("prometheus");
        }
    }

    // ==================== Native Metrics Helpers ====================

    private Map<String, Object> safeGetNativeClusterMetrics() {
        try {
            return metricsService.getClusterMetrics();
        } catch (Exception e) {
            log.debug("Native cluster metrics unavailable: {}", e.getMessage());
            return unavailableResult("native");
        }
    }

    private Map<String, Object> safeGetNativeBrokerMetrics(String brokerName) {
        try {
            return metricsService.getBrokerMetrics(brokerName);
        } catch (Exception e) {
            log.debug("Native broker metrics unavailable for {}: {}", brokerName, e.getMessage());
            return unavailableResult("native");
        }
    }

    private Map<String, Object> safeGetNativeTopicMetrics(String topicName) {
        try {
            return metricsService.getTopicMetrics(topicName);
        } catch (Exception e) {
            log.debug("Native topic metrics unavailable for {}: {}", topicName, e.getMessage());
            return unavailableResult("native");
        }
    }

    private Map<String, Object> safeGetNativeConsumerMetrics(String groupName) {
        try {
            return metricsService.getConsumerGroupMetrics(groupName);
        } catch (Exception e) {
            log.debug("Native consumer metrics unavailable for {}: {}", groupName, e.getMessage());
            return unavailableResult("native");
        }
    }

    private Map<String, Object> safeGetNativeSystemMetrics() {
        try {
            return metricsService.getSystemMetrics();
        } catch (Exception e) {
            log.debug("Native system metrics unavailable: {}", e.getMessage());
            return unavailableResult("native");
        }
    }

    // ==================== Utility Methods ====================

    /**
     * Extract the "metrics" sub-map from a Prometheus query client response.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> extractMetricsFromPrometheusResponse(Map<String, Object> prometheusResponse) {
        if (prometheusResponse == null) return Map.of();
        Object metrics = prometheusResponse.get("metrics");
        if (metrics instanceof Map) {
            return (Map<String, Object>) metrics;
        }
        return Map.of();
    }

    /**
     * Merge source metrics into the target map, with source values taking
     * precedence only for keys not already present (native data wins).
     */
    private void mergeMetrics(Map<String, Object> target, Map<String, Object> source) {
        if (source == null) return;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            if (entry.getValue() != null && !target.containsKey(key)) {
                target.put(key, entry.getValue());
            }
        }
    }

    /**
     * Build a result map indicating a data source is unavailable.
     */
    private Map<String, Object> unavailableResult(String source) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("source", source);
        result.put("available", false);
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
}
