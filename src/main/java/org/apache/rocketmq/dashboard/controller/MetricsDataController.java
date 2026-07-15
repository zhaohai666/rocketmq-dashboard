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
package org.apache.rocketmq.dashboard.controller;

import org.apache.rocketmq.dashboard.service.metrics.MetricsAggregationService;
import org.apache.rocketmq.dashboard.service.metrics.PrometheusMetricsQueryClient;
import org.apache.rocketmq.dashboard.support.JsonResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Metrics Data Controller providing aggregated metrics endpoints for the
 * RocketMQ Dashboard.
 *
 * <p>This controller combines data from Prometheus (via PromQL queries) and the
 * native MetadataProvider to deliver a unified metrics view. It serves as the
 * primary API for the Dashboard frontend to retrieve monitoring data.</p>
 *
 * <h3>Endpoints</h3>
 * <table border="1">
 *   <tr><th>Method</th><th>Path</th><th>Description</th></tr>
 *   <tr><td>GET</td><td>/api/metrics/prometheus/query</td><td>Proxy PromQL instant queries to Prometheus</td></tr>
 *   <tr><td>GET</td><td>/api/metrics/prometheus/range_query</td><td>Proxy PromQL range queries to Prometheus</td></tr>
 *   <tr><td>GET</td><td>/api/metrics/dashboard</td><td>Get aggregated dashboard metrics</td></tr>
 *   <tr><td>GET</td><td>/api/metrics/broker/{name}</td><td>Get broker-specific aggregated metrics</td></tr>
 *   <tr><td>GET</td><td>/api/metrics/topic/{name}</td><td>Get topic-specific aggregated metrics</td></tr>
 *   <tr><td>GET</td><td>/api/metrics/consumer/{group}</td><td>Get consumer group aggregated metrics</td></tr>
 *   <tr><td>GET</td><td>/api/metrics/system</td><td>Get system-level aggregated metrics</td></tr>
 *   <tr><td>GET</td><td>/api/metrics/health</td><td>Get data sources health status</td></tr>
 *   <tr><td>GET</td><td>/api/metrics/templates</td><td>Get pre-built PromQL query templates</td></tr>
 * </table>
 *
 * <p>Part of RIP-1 METRICS-01: Prometheus/OpenMetrics integration for
 * standardized RocketMQ monitoring.</p>
 */
@RestController
@RequestMapping("/api/metrics")
public class MetricsDataController {

    private static final Logger log = LoggerFactory.getLogger(MetricsDataController.class);

    @Resource
    private MetricsAggregationService metricsAggregationService;

    @Resource
    private PrometheusMetricsQueryClient prometheusClient;

    // ==================== PromQL Proxy ====================

    /**
     * GET /api/metrics/prometheus/query - Proxy PromQL instant queries to Prometheus.
     *
     * <p>Forwards a PromQL expression to the configured Prometheus server and
     * returns the parsed JSON response. Supports optional evaluation time and
     * datasource selection.</p>
     *
     * @param query        PromQL query expression (required)
     * @param time         evaluation timestamp - Unix timestamp or RFC3339 (optional)
     * @param datasourceId datasource ID to query (optional, uses default if omitted)
     * @return JsonResult containing the Prometheus query response
     */
    @GetMapping("/prometheus/query")
    public Object prometheusQuery(
            @RequestParam String query,
            @RequestParam(required = false) String time,
            @RequestParam(required = false) String datasourceId) {
        try {
            if (query == null || query.trim().isEmpty()) {
                return new JsonResult<>(1, "Query parameter is required");
            }

            Map<String, Object> result;
            if (time != null && !time.trim().isEmpty()) {
                result = prometheusClient.queryInstant(query.trim(), time.trim(), datasourceId);
            } else {
                result = prometheusClient.queryInstant(query.trim(), datasourceId);
            }

            return new JsonResult<>(result);
        } catch (Exception e) {
            log.error("Failed to execute PromQL query: {}", query, e);
            return new JsonResult<>(1, "Failed to execute PromQL query: " + e.getMessage());
        }
    }

    /**
     * GET /api/metrics/prometheus/range_query - Proxy PromQL range queries to Prometheus.
     *
     * <p>Forwards a PromQL range query to the configured Prometheus server
     * and returns the parsed matrix response.</p>
     *
     * @param query        PromQL query expression (required)
     * @param start        start timestamp (required)
     * @param end          end timestamp (required)
     * @param step         query resolution step, e.g. "15s", "1m" (required)
     * @param datasourceId datasource ID (optional)
     * @return JsonResult containing the Prometheus range query response
     */
    @GetMapping("/prometheus/range_query")
    public Object prometheusRangeQuery(
            @RequestParam String query,
            @RequestParam String start,
            @RequestParam String end,
            @RequestParam String step,
            @RequestParam(required = false) String datasourceId) {
        try {
            if (query == null || query.trim().isEmpty()) {
                return new JsonResult<>(1, "Query parameter is required");
            }
            if (start == null || start.trim().isEmpty()) {
                return new JsonResult<>(1, "Start parameter is required");
            }
            if (end == null || end.trim().isEmpty()) {
                return new JsonResult<>(1, "End parameter is required");
            }
            if (step == null || step.trim().isEmpty()) {
                return new JsonResult<>(1, "Step parameter is required");
            }

            Map<String, Object> result = prometheusClient.queryRange(
                    query.trim(), start.trim(), end.trim(), step.trim(), datasourceId);
            return new JsonResult<>(result);
        } catch (Exception e) {
            log.error("Failed to execute PromQL range query: {}", query, e);
            return new JsonResult<>(1, "Failed to execute PromQL range query: " + e.getMessage());
        }
    }

    // ==================== Aggregated Dashboard ====================

    /**
     * GET /api/metrics/dashboard - Get aggregated dashboard metrics.
     *
     * <p>Returns a comprehensive metrics view combining data from both Prometheus
     * (time-series metrics via PromQL) and the native MetadataProvider (operational
     * data from the RocketMQ admin API). This is the primary endpoint for the
     * Dashboard overview page.</p>
     *
     * @return JsonResult containing aggregated dashboard metrics
     */
    @GetMapping("/dashboard")
    public Object getDashboardMetrics() {
        try {
            Map<String, Object> dashboard = metricsAggregationService.getDashboardMetrics();
            return new JsonResult<>(dashboard);
        } catch (Exception e) {
            log.error("Failed to get dashboard metrics", e);
            return new JsonResult<>(1, "Failed to get dashboard metrics: " + e.getMessage());
        }
    }

    // ==================== Component-Specific Metrics ====================

    /**
     * GET /api/metrics/broker/{name} - Get broker-specific aggregated metrics.
     *
     * <p>Returns combined metrics for a specific broker, including:</p>
     * <ul>
     *   <li>Send/born TPS from Prometheus</li>
     *   <li>Disk usage ratio</li>
     *   <li>JVM heap and GC statistics</li>
     *   <li>Remoting latency percentiles</li>
     *   <li>Native broker runtime stats from MetadataProvider</li>
     * </ul>
     *
     * @param name the broker name (path variable)
     * @return JsonResult containing aggregated broker metrics
     */
    @GetMapping("/broker/{name}")
    public Object getBrokerMetrics(@PathVariable String name) {
        try {
            if (name == null || name.trim().isEmpty()) {
                return new JsonResult<>(1, "Broker name is required");
            }
            Map<String, Object> metrics = metricsAggregationService.getAggregatedBrokerMetrics(name.trim());
            return new JsonResult<>(metrics);
        } catch (Exception e) {
            log.error("Failed to get broker metrics for: {}", name, e);
            return new JsonResult<>(1, "Failed to get broker metrics: " + e.getMessage());
        }
    }

    /**
     * GET /api/metrics/topic/{name} - Get topic-specific aggregated metrics.
     *
     * <p>Returns combined metrics for a specific topic, including:</p>
     * <ul>
     *   <li>Message put rate and size rate</li>
     *   <li>Error message rate</li>
     *   <li>Per-consumer-group consumption rates</li>
     *   <li>Accumulation depth per consumer group</li>
     *   <li>Native topic metadata from MetadataProvider</li>
     * </ul>
     *
     * @param name the topic name (path variable)
     * @return JsonResult containing aggregated topic metrics
     */
    @GetMapping("/topic/{name}")
    public Object getTopicMetrics(@PathVariable String name) {
        try {
            if (name == null || name.trim().isEmpty()) {
                return new JsonResult<>(1, "Topic name is required");
            }
            Map<String, Object> metrics = metricsAggregationService.getAggregatedTopicMetrics(name.trim());
            return new JsonResult<>(metrics);
        } catch (Exception e) {
            log.error("Failed to get topic metrics for: {}", name, e);
            return new JsonResult<>(1, "Failed to get topic metrics: " + e.getMessage());
        }
    }

    /**
     * GET /api/metrics/consumer/{group} - Get consumer group aggregated metrics.
     *
     * <p>Returns combined metrics for a specific consumer group, including:</p>
     * <ul>
     *   <li>Consumer offset and lag (diff)</li>
     *   <li>Pull latency maximum</li>
     *   <li>Consumption TPS</li>
     *   <li>Error message consumption rate</li>
     *   <li>Native consumer group info from MetadataProvider</li>
     * </ul>
     *
     * @param group the consumer group name (path variable)
     * @return JsonResult containing aggregated consumer group metrics
     */
    @GetMapping("/consumer/{group}")
    public Object getConsumerGroupMetrics(@PathVariable String group) {
        try {
            if (group == null || group.trim().isEmpty()) {
                return new JsonResult<>(1, "Consumer group name is required");
            }
            Map<String, Object> metrics = metricsAggregationService.getAggregatedConsumerGroupMetrics(group.trim());
            return new JsonResult<>(metrics);
        } catch (Exception e) {
            log.error("Failed to get consumer group metrics for: {}", group, e);
            return new JsonResult<>(1, "Failed to get consumer group metrics: " + e.getMessage());
        }
    }

    // ==================== System Metrics ====================

    /**
     * GET /api/metrics/system - Get system-level aggregated metrics.
     *
     * <p>Returns combined system metrics including JVM stats, process info,
     * CPU usage, and infrastructure health across all RocketMQ components.</p>
     *
     * @return JsonResult containing aggregated system metrics
     */
    @GetMapping("/system")
    public Object getSystemMetrics() {
        try {
            Map<String, Object> metrics = metricsAggregationService.getAggregatedSystemMetrics();
            return new JsonResult<>(metrics);
        } catch (Exception e) {
            log.error("Failed to get system metrics", e);
            return new JsonResult<>(1, "Failed to get system metrics: " + e.getMessage());
        }
    }

    // ==================== Health & Utility ====================

    /**
     * GET /api/metrics/health - Get data sources health status.
     *
     * <p>Returns the health status of all configured metrics data sources
     * (Prometheus, native MetadataProvider) including connectivity status,
     * latency, and available metric families.</p>
     *
     * @return JsonResult containing health status for each data source
     */
    @GetMapping("/health")
    public Object getDataSourcesHealth() {
        try {
            Map<String, Object> health = metricsAggregationService.getDataSourcesHealth();
            return new JsonResult<>(health);
        } catch (Exception e) {
            log.error("Failed to get data sources health", e);
            return new JsonResult<>(1, "Failed to get data sources health: " + e.getMessage());
        }
    }

    /**
     * GET /api/metrics/templates - Get pre-built PromQL query templates.
     *
     * <p>Returns the standard PromQL query templates organized by category
     * (cluster, broker, topic, consumer, system). These templates use the
     * standard RocketMQ exporter metric names.</p>
     *
     * @return JsonResult containing query templates grouped by category
     */
    @GetMapping("/templates")
    public Object getQueryTemplates() {
        try {
            Map<String, Map<String, String>> templates = prometheusClient.getQueryTemplates();
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("templates", templates);
            result.put("description", "Pre-built PromQL query templates for RocketMQ monitoring. "
                    + "Use %s placeholders in broker/topic/consumer queries to substitute entity names.");
            return new JsonResult<>(result);
        } catch (Exception e) {
            log.error("Failed to get query templates", e);
            return new JsonResult<>(1, "Failed to get query templates: " + e.getMessage());
        }
    }
}
