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
package org.apache.rocketmq.dashboard.service.impl;

import apache.rocketmq.proxy.admin.v1.BatchConsumeClientDiagnostics;
import apache.rocketmq.proxy.admin.v1.BatchConsumeGroupSummary;
import apache.rocketmq.proxy.admin.v1.DescribeBatchConsumeDiagnosticsResponse;
import apache.rocketmq.proxy.admin.v1.DescribePopReceiptHandlesResponse;
import apache.rocketmq.proxy.admin.v1.PopReceiptHandleGroupSummary;
import apache.rocketmq.proxy.admin.v1.PopReceiptHandleInfo;
import apache.rocketmq.proxy.admin.v1.ProxyRuntimeConfig;
import apache.rocketmq.proxy.admin.v1.UpdateConfigResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.dashboard.service.ProxyAdminService;
import org.apache.rocketmq.dashboard.service.client.MultiProxyAdminClient;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of ProxyAdminService that delegates to MultiProxyAdminClient
 * for aggregated multi-Proxy operations.
 *
 * <p>Converts gRPC proto objects to generic Map representations for
 * REST API serialization without coupling the controller layer to proto types.</p>
 */
@Slf4j
@Service
public class ProxyAdminServiceImpl implements ProxyAdminService {

    @Resource
    private MultiProxyAdminClient multiProxyAdminClient;

    // ==================== M2: Config & Connection Management ====================

    @Override
    public Map<String, Map<String, Object>> getProxyConfigs() {
        if (multiProxyAdminClient == null || !multiProxyAdminClient.isAvailable()) {
            log.warn("[PROXY-ADMIN] MultiProxyAdminClient not available");
            return Collections.emptyMap();
        }

        Map<String, ProxyRuntimeConfig> rawConfigs = multiProxyAdminClient.getConfigFromAll();
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();

        for (Map.Entry<String, ProxyRuntimeConfig> entry : rawConfigs.entrySet()) {
            result.put(entry.getKey(), convertConfigToMap(entry.getValue()));
        }

        return result;
    }

    @Override
    public Map<String, List<String>> updateProxyConfig(Map<String, Object> configUpdates) {
        if (multiProxyAdminClient == null || !multiProxyAdminClient.isAvailable()) {
            log.warn("[PROXY-ADMIN] MultiProxyAdminClient not available for config update");
            return Collections.emptyMap();
        }

        ProxyRuntimeConfig.Builder builder = ProxyRuntimeConfig.newBuilder();
        applyConfigUpdates(builder, configUpdates);

        Map<String, UpdateConfigResponse> responses = multiProxyAdminClient.updateConfigAll(builder.build());
        Map<String, List<String>> result = new LinkedHashMap<>();

        for (Map.Entry<String, UpdateConfigResponse> entry : responses.entrySet()) {
            if (entry.getValue() != null) {
                result.put(entry.getKey(), entry.getValue().getChangedFieldsList());
            } else {
                result.put(entry.getKey(), Collections.emptyList());
            }
        }

        return result;
    }

    @Override
    public boolean disconnectClient(String clientId, String reason) {
        if (multiProxyAdminClient == null || !multiProxyAdminClient.isAvailable()) {
            log.warn("[PROXY-ADMIN] MultiProxyAdminClient not available for disconnect");
            return false;
        }

        return multiProxyAdminClient.disconnectClient(clientId, reason);
    }

    // ==================== M3: POP Diagnostics ====================

    @Override
    public PopDiagnosticsResult describePopReceiptHandles(String group, String topic,
                                                           int pageNum, int pageSize) {
        PopDiagnosticsResult result = new PopDiagnosticsResult();
        result.setHandles(new ArrayList<>());

        if (multiProxyAdminClient == null || !multiProxyAdminClient.isAvailable()) {
            log.warn("[PROXY-ADMIN] MultiProxyAdminClient not available for POP diagnostics");
            return result;
        }

        List<DescribePopReceiptHandlesResponse> responses =
            multiProxyAdminClient.describePopReceiptHandlesFromAll(group, topic, pageNum, pageSize);

        int totalHandles = 0, totalMessages = 0, expiredHandles = 0;
        long totalRenewTimes = 0, totalRenewRetryTimes = 0;

        for (DescribePopReceiptHandlesResponse resp : responses) {
            if (resp.hasSummary()) {
                PopReceiptHandleGroupSummary summary = resp.getSummary();
                totalHandles += summary.getTotalHandles();
                totalMessages += summary.getTotalMessages();
                totalRenewTimes += summary.getTotalRenewTimes();
                totalRenewRetryTimes += summary.getTotalRenewRetryTimes();
                expiredHandles += summary.getExpiredHandles();
            }

            for (PopReceiptHandleInfo handle : resp.getHandlesList()) {
                result.getHandles().add(convertPopHandleToMap(handle));
            }
        }

        result.setTotalHandles(totalHandles);
        result.setTotalMessages(totalMessages);
        result.setTotalRenewTimes(totalRenewTimes);
        result.setTotalRenewRetryTimes(totalRenewRetryTimes);
        result.setExpiredHandles(expiredHandles);
        result.setTotal(result.getHandles().size());

        return result;
    }

    // ==================== M4: Batch Consumption Diagnostics ====================

    @Override
    public BatchConsumeDiagnosticsResult describeBatchConsumeDiagnostics(
            String group, String topic, String clientId, int pageNum, int pageSize) {
        BatchConsumeDiagnosticsResult result = new BatchConsumeDiagnosticsResult();
        result.setDiagnostics(new ArrayList<>());

        if (multiProxyAdminClient == null || !multiProxyAdminClient.isAvailable()) {
            log.warn("[PROXY-ADMIN] MultiProxyAdminClient not available for batch diagnostics");
            return result;
        }

        List<DescribeBatchConsumeDiagnosticsResponse> responses =
            multiProxyAdminClient.describeBatchConsumeDiagnosticsFromAll(
                group, topic, clientId, pageNum, pageSize);

        int totalClients = 0, totalUnacked = 0, totalHandles = 0, totalExpired = 0;
        long totalRenew = 0, totalRetry = 0;

        for (DescribeBatchConsumeDiagnosticsResponse resp : responses) {
            if (resp.hasSummary()) {
                BatchConsumeGroupSummary summary = resp.getSummary();
                totalClients += summary.getTotalClients();
                totalUnacked += summary.getTotalUnackedMessages();
                totalHandles += summary.getTotalUnackedHandles();
                totalExpired += summary.getTotalExpiredHandles();
                totalRenew += summary.getTotalRenewTimes();
                totalRetry += summary.getTotalRenewRetryTimes();
            }

            for (BatchConsumeClientDiagnostics diag : resp.getDiagnosticsList()) {
                result.getDiagnostics().add(convertBatchDiagToMap(diag));
            }
        }

        result.setTotalClients(totalClients);
        result.setTotalUnackedMessages(totalUnacked);
        result.setTotalUnackedHandles(totalHandles);
        result.setTotalExpiredHandles(totalExpired);
        result.setTotalRenewTimes(totalRenew);
        result.setTotalRenewRetryTimes(totalRetry);
        result.setTotal(result.getDiagnostics().size());

        return result;
    }

    // ==================== Status ====================

    @Override
    public ProxyAdminStatus getStatus() {
        ProxyAdminStatus status = new ProxyAdminStatus();

        if (multiProxyAdminClient == null) {
            status.setConnected(false);
            status.setAvailableProxies(0);
            status.setTotalProxies(0);
            status.setProxyAddresses(new String[0]);
            return status;
        }

        status.setConnected(multiProxyAdminClient.isAvailable());
        status.setAvailableProxies(multiProxyAdminClient.getAvailableCount());
        status.setTotalProxies(multiProxyAdminClient.getTotalCount());
        status.setProxyAddresses(multiProxyAdminClient.getProxyAddresses());

        return status;
    }

    // ==================== Private Converters ====================

    private Map<String, Object> convertConfigToMap(ProxyRuntimeConfig config) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("proxyMode", config.getProxyMode());
        map.put("rocketmqClusterName", config.getRocketmqClusterName());
        map.put("proxyClusterName", config.getProxyClusterName());
        map.put("proxyName", config.getProxyName());
        map.put("localServeAddr", config.getLocalServeAddr());
        map.put("namesrvAddr", config.getNamesrvAddr());
        map.put("grpcServerPort", config.getGrpcServerPort());
        map.put("proxyAdminEnabled", config.getProxyAdminEnabled());
        map.put("proxyAdminServerPort", config.getProxyAdminServerPort());
        map.put("proxyAdminThreadPoolNums", config.getProxyAdminThreadPoolNums());
        map.put("proxyAdminMaxPageSize", config.getProxyAdminMaxPageSize());
        map.put("proxyAdminDescribeClientConcurrencyLimit", config.getProxyAdminDescribeClientConcurrencyLimit());
        map.put("proxyAdminSamplingRateUnderLoad", config.getProxyAdminSamplingRateUnderLoad());
        map.put("maxMessageSize", config.getMaxMessageSize());
        map.put("defaultInvisibleTimeMills", config.getDefaultInvisibleTimeMills());
        map.put("maxInvisibleTimeMills", config.getMaxInvisibleTimeMills());
        map.put("metricsExporterType", config.getMetricsExporterType());
        map.put("metricsPromExporterPort", config.getMetricsPromExporterPort());
        map.put("traceOn", config.getTraceOn());
        map.put("tlsTestModeEnable", config.getTlsTestModeEnable());
        return map;
    }

    private void applyConfigUpdates(ProxyRuntimeConfig.Builder builder, Map<String, Object> updates) {
        for (Map.Entry<String, Object> entry : updates.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();

            switch (field) {
                case "proxyAdminThreadPoolNums":
                    builder.setProxyAdminThreadPoolNums(((Number) value).intValue());
                    break;
                case "proxyAdminMaxPageSize":
                    builder.setProxyAdminMaxPageSize(((Number) value).intValue());
                    break;
                case "proxyAdminDescribeClientConcurrencyLimit":
                    builder.setProxyAdminDescribeClientConcurrencyLimit(((Number) value).intValue());
                    break;
                case "proxyAdminSamplingRateUnderLoad":
                    builder.setProxyAdminSamplingRateUnderLoad(((Number) value).doubleValue());
                    break;
                case "maxMessageSize":
                    builder.setMaxMessageSize(((Number) value).intValue());
                    break;
                case "defaultInvisibleTimeMills":
                    builder.setDefaultInvisibleTimeMills(((Number) value).longValue());
                    break;
                case "maxInvisibleTimeMills":
                    builder.setMaxInvisibleTimeMills(((Number) value).longValue());
                    break;
                case "traceOn":
                    builder.setTraceOn((Boolean) value);
                    break;
                case "proxyAdminEnabled":
                    builder.setProxyAdminEnabled((Boolean) value);
                    break;
                default:
                    log.warn("[PROXY-ADMIN] Unknown config field: {}", field);
                    break;
            }
        }
    }

    private Map<String, Object> convertPopHandleToMap(PopReceiptHandleInfo handle) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("group", handle.getGroup());
        map.put("topic", handle.getTopic());
        map.put("queueId", handle.getQueueId());
        map.put("messageId", handle.getMessageId());
        map.put("queueOffset", handle.getQueueOffset());
        map.put("reconsumeTimes", handle.getReconsumeTimes());
        map.put("renewTimes", handle.getRenewTimes());
        map.put("renewRetryTimes", handle.getRenewRetryTimes());
        map.put("consumeTimestamp", handle.getConsumeTimestamp());
        map.put("receiptHandle", handle.getReceiptHandle());
        map.put("nextVisibleTime", handle.getNextVisibleTime());
        map.put("invisibleTime", handle.getInvisibleTime());
        map.put("brokerName", handle.getBrokerName());
        map.put("isExpired", handle.getIsExpired());
        return map;
    }

    private Map<String, Object> convertBatchDiagToMap(BatchConsumeClientDiagnostics diag) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("clientId", diag.getClientId());
        map.put("channelId", diag.getChannelId());
        map.put("unackedMessageCount", diag.getUnackedMessageCount());
        map.put("unackedHandleCount", diag.getUnackedHandleCount());
        map.put("totalRenewTimes", diag.getTotalRenewTimes());
        map.put("totalRenewRetryTimes", diag.getTotalRenewRetryTimes());
        map.put("expiredHandleCount", diag.getExpiredHandleCount());
        map.put("topicDistribution", diag.getTopicDistributionMap());
        map.put("consumeType", diag.getConsumeType());
        map.put("messageModel", diag.getMessageModel());
        map.put("receiveBatchSize", diag.getReceiveBatchSize());
        map.put("longPollingTimeoutMs", diag.getLongPollingTimeoutMs());
        map.put("lastRttMs", diag.getLastRttMs());
        map.put("connectTime", diag.getConnectTime());
        return map;
    }
}
