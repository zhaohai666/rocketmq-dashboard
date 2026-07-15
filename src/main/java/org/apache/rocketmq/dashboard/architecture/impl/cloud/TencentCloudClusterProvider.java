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
package org.apache.rocketmq.dashboard.architecture.impl.cloud;

import org.apache.rocketmq.dashboard.architecture.impl.cloud.CloudApiHttpClient.CloudApiException;
import org.apache.rocketmq.dashboard.model.CloudProviderConfig;
import org.apache.rocketmq.dashboard.model.ClusterCapability;
import org.apache.rocketmq.dashboard.model.ClusterTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tencent Cloud (TDMQ) RocketMQ cluster provider implementation.
 *
 * <p>Tencent Cloud TDMQ for RocketMQ provides a managed RocketMQ service
 * with OpenAPI access via tdmq.tencentcloudapi.com.</p>
 *
 * <h3>Tencent Cloud TDMQ API Mapping</h3>
 * <ul>
 *   <li>DescribeRocketMQClusters → Cluster topology discovery</li>
 *   <li>DescribeRocketMQNamespaces → Namespace listing</li>
 *   <li>DescribeRocketMQTopics → Topic listing</li>
 *   <li>CreateRocketMQTopic → Topic creation</li>
 *   <li>DeleteRocketMQTopic → Topic deletion</li>
 *   <li>DescribeRocketMQGroups → Consumer group listing</li>
 * </ul>
 *
 * <h3>TC3-HMAC-SHA256 Signing</h3>
 * <p>Uses Tencent Cloud API 3.0 signature (TC3-HMAC-SHA256) for authentication.
 * All requests are POST with JSON body to the service endpoint.</p>
 *
 * <p>Per RIP-1 ARCH-01 §5.1 M1, this adapter enables unified multi-cluster
 * management for Tencent Cloud-hosted RocketMQ instances.</p>
 */
public class TencentCloudClusterProvider extends AbstractCloudClusterProvider {

    private static final Logger log = LoggerFactory.getLogger(TencentCloudClusterProvider.class);

    /** Tencent Cloud TDMQ API endpoint. */
    private static final String DEFAULT_ENDPOINT = "tdmq.tencentcloudapi.com";

    /** Tencent Cloud TDMQ service name for signing. */
    private static final String SERVICE = "tdmq";

    /** Tencent Cloud API version. */
    private static final String API_VERSION = "2020-02-17";

    /** Resolved API endpoint. */
    private String resolvedEndpoint;

    /** Shared HTTP client for API calls. */
    private CloudApiHttpClient httpClient;

    public TencentCloudClusterProvider(CloudProviderConfig config) {
        super(config);
        log.info("TencentCloudClusterProvider created for instance: {}", config.getInstanceId());
    }

    @Override
    protected void doInitialize() throws Exception {
        log.info("Initializing Tencent Cloud TDMQ client for region: {}", config.getRegionId());
        this.httpClient = new CloudApiHttpClient();
        this.resolvedEndpoint = config.getEndpoint() != null && !config.getEndpoint().trim().isEmpty()
            ? config.getEndpoint() : DEFAULT_ENDPOINT;

        // Validate credentials by calling a lightweight API
        try {
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("ClusterId", config.getInstanceId());
            params.put("Offset", 0);
            params.put("Limit", 1);
            callApi("DescribeRocketMQClusters", params);
            log.info("Tencent Cloud TDMQ client initialized: instance={}, region={}, endpoint={}",
                config.getInstanceId(), config.getRegionId(), resolvedEndpoint);
        } catch (CloudApiException e) {
            log.warn("Tencent Cloud credential validation returned error (may still work): {}", e.getMessage());
            log.info("Tencent Cloud TDMQ client initialized (with warning): instance={}, region={}, endpoint={}",
                config.getInstanceId(), config.getRegionId(), resolvedEndpoint);
        }
    }

    @Override
    protected void doShutdown() {
        log.info("Shutting down Tencent Cloud TDMQ client for instance: {}", config.getInstanceId());
    }

    @Override
    protected ClusterTopology doDiscoverTopology() throws Exception {
        log.info("Discovering topology for Tencent Cloud instance: {}", config.getInstanceId());
        ClusterTopology topology = new ClusterTopology();
        topology.setClusterName(config.getInstanceId());

        try {
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("ClusterIdList", List.of(config.getInstanceId()));
            params.put("Offset", 0);
            params.put("Limit", 20);
            Map<String, Object> result = callApi("DescribeRocketMQClusters", params);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> clusterList =
                (List<Map<String, Object>>) result.get("ClusterList");
            if (clusterList != null && !clusterList.isEmpty()) {
                Map<String, Object> cluster = clusterList.get(0);
                @SuppressWarnings("unchecked")
                Map<String, Object> info = (Map<String, Object>) cluster.get("Info");
                if (info != null) {
                    String clusterName = CloudApiHttpClient.getString(info, "ClusterName");
                    if (clusterName != null) {
                        topology.setClusterName(clusterName);
                    }
                    String publicEndpoint = CloudApiHttpClient.getString(info, "PublicEndPoint");
                    String vpcEndpoint = CloudApiHttpClient.getString(info, "VpcEndPoint");

                    String nodeAddr = vpcEndpoint != null ? vpcEndpoint :
                        (publicEndpoint != null ? publicEndpoint : resolvedEndpoint);
                    topology.addNode(config.getInstanceId(), 0L, nodeAddr, "BROKER");

                    if (publicEndpoint != null) {
                        topology.getNamesrvAddresses().add(publicEndpoint);
                    }
                    if (vpcEndpoint != null) {
                        topology.getNamesrvAddresses().add(vpcEndpoint);
                    }
                }

                @SuppressWarnings("unchecked")
                Map<String, Object> status = (Map<String, Object>) cluster.get("Status");
                if (status != null) {
                    int isOnline = CloudApiHttpClient.getInt(status, "IsOnline", 0);
                    if (!topology.getBrokerNodes().isEmpty()) {
                        topology.getBrokerNodes().get(0).setStatus(isOnline == 1 ? "ONLINE" : "OFFLINE");
                    }
                }
            } else {
                topology.addNode(config.getInstanceId(), 0L, resolvedEndpoint, "BROKER");
            }
        } catch (CloudApiException e) {
            log.error("Failed to discover Tencent Cloud topology for instance {}: {}",
                config.getInstanceId(), e.getMessage());
            topology.addNode(config.getInstanceId(), 0L, resolvedEndpoint, "BROKER");
        }

        return topology;
    }

    @Override
    protected ClusterCapability doDetectCapability() throws Exception {
        log.info("Detecting capability for Tencent Cloud instance: {}", config.getInstanceId());
        ClusterCapability capability = new ClusterCapability();
        capability.setNamespaceSupported(true);
        capability.setLiteTopicSupported(true);
        capability.setPopConsumeSupported(true);
        capability.setAclV2Supported(true);
        capability.setGrpcClientSupported(false);
        capability.setDelayMessageSupported(true);
        capability.setTransactionMessageSupported(true);
        capability.setFifoMessageSupported(true);
        capability.setArchitectureVersion("5.x-cloud");
        capability.setRocketmqVersion("5.x-tencent");
        capability.setExtendedCapabilities(new HashSet<>());
        capability.getExtendedCapabilities().add("topicTypeValidation");
        capability.getExtendedCapabilities().add("metricsQuery");
        capability.getExtendedCapabilities().add("clientTrace");

        // Try to detect actual version from cluster info
        try {
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("ClusterIdList", List.of(config.getInstanceId()));
            params.put("Offset", 0);
            params.put("Limit", 1);
            Map<String, Object> result = callApi("DescribeRocketMQClusters", params);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> clusterList =
                (List<Map<String, Object>>) result.get("ClusterList");
            if (clusterList != null && !clusterList.isEmpty()) {
                Map<String, Object> cluster = clusterList.get(0);
                @SuppressWarnings("unchecked")
                Map<String, Object> info = (Map<String, Object>) cluster.get("Info");
                if (info != null) {
                    String version = CloudApiHttpClient.getString(info, "Remark");
                    if (version != null && !version.isEmpty()) {
                        capability.setRocketmqVersion(version + "-tencent");
                    }
                }
            }
        } catch (CloudApiException e) {
            log.debug("Could not detect Tencent Cloud version from API, using defaults: {}", e.getMessage());
        }

        return capability;
    }

    @Override
    protected List<String> doListNodeIds() throws Exception {
        log.info("Listing node IDs for Tencent Cloud instance: {}", config.getInstanceId());
        List<String> nodeIds = new ArrayList<>();
        nodeIds.add(config.getInstanceId());

        try {
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("ClusterIdList", List.of(config.getInstanceId()));
            params.put("Offset", 0);
            params.put("Limit", 1);
            Map<String, Object> result = callApi("DescribeRocketMQClusters", params);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> clusterList =
                (List<Map<String, Object>>) result.get("ClusterList");
            if (clusterList != null && !clusterList.isEmpty()) {
                Map<String, Object> cluster = clusterList.get(0);
                @SuppressWarnings("unchecked")
                Map<String, Object> info = (Map<String, Object>) cluster.get("Info");
                if (info != null) {
                    String vpcEndpoint = CloudApiHttpClient.getString(info, "VpcEndPoint");
                    if (vpcEndpoint != null) {
                        nodeIds.add("endpoint:" + vpcEndpoint);
                    }
                }
            }
        } catch (CloudApiException e) {
            log.warn("Failed to list Tencent Cloud nodes: {}", e.getMessage());
        }

        return nodeIds;
    }

    @Override
    protected boolean doHealthCheck() throws Exception {
        log.info("Health check for Tencent Cloud instance: {}", config.getInstanceId());
        try {
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("ClusterIdList", List.of(config.getInstanceId()));
            params.put("Offset", 0);
            params.put("Limit", 1);
            Map<String, Object> result = callApi("DescribeRocketMQClusters", params);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> clusterList =
                (List<Map<String, Object>>) result.get("ClusterList");
            if (clusterList != null && !clusterList.isEmpty()) {
                Map<String, Object> cluster = clusterList.get(0);
                @SuppressWarnings("unchecked")
                Map<String, Object> status = (Map<String, Object>) cluster.get("Status");
                if (status != null) {
                    int isOnline = CloudApiHttpClient.getInt(status, "IsOnline", 0);
                    return isOnline == 1;
                }
            }
            return true;
        } catch (CloudApiException e) {
            log.error("Tencent Cloud health check failed: {}", e.getMessage());
            return false;
        }
    }

    // ==================== Tencent Cloud TC3-HMAC-SHA256 API Signing ====================

    /**
     * Call a Tencent Cloud TDMQ API action using TC3-HMAC-SHA256 signing.
     *
     * @param action the API action (e.g., "DescribeRocketMQClusters")
     * @param params request body parameters
     * @return parsed JSON Response field from the API response
     */
    Map<String, Object> callApi(String action, Map<String, Object> params) throws CloudApiException {
        long timestamp = Instant.now().getEpochSecond();
        String date = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .withZone(ZoneOffset.UTC)
            .format(Instant.ofEpochSecond(timestamp));

        String jsonBody = CloudApiHttpClient.toJson(params != null ? params : new HashMap<>());
        String hashedPayload = CloudApiHttpClient.sha256Hex(jsonBody);

        // Step 1: Build Canonical Request
        String canonicalRequest = "POST\n"
            + "/\n"
            + "\n"
            + "content-type:application/json\n"
            + "host:" + resolvedEndpoint + "\n"
            + "x-tc-action:" + action.toLowerCase() + "\n"
            + "\n"
            + "content-type;host;x-tc-action\n"
            + hashedPayload;

        // Step 2: Build String to Sign
        String credentialScope = date + "/" + SERVICE + "/tc3_request";
        String hashedCanonicalRequest = CloudApiHttpClient.sha256Hex(canonicalRequest);
        String stringToSign = "TC3-HMAC-SHA256\n"
            + timestamp + "\n"
            + credentialScope + "\n"
            + hashedCanonicalRequest;

        // Step 3: Calculate Signature
        byte[] secretDate = CloudApiHttpClient.hmacSha256(
            ("TC3" + config.getSecretKey()).getBytes(StandardCharsets.UTF_8), date);
        byte[] secretService = CloudApiHttpClient.hmacSha256(secretDate, SERVICE);
        byte[] secretSigning = CloudApiHttpClient.hmacSha256(secretService, "tc3_request");
        String signature = CloudApiHttpClient.hmacSha256Hex(secretSigning, stringToSign);

        // Step 4: Build Authorization header
        String authorization = "TC3-HMAC-SHA256"
            + " Credential=" + config.getAccessKey() + "/" + credentialScope
            + ", SignedHeaders=content-type;host;x-tc-action"
            + ", Signature=" + signature;

        // Step 5: Execute request
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Authorization", authorization);
        headers.put("Host", resolvedEndpoint);
        headers.put("X-TC-Action", action);
        headers.put("X-TC-Timestamp", String.valueOf(timestamp));
        headers.put("X-TC-Version", API_VERSION);
        headers.put("X-TC-Region", config.getRegionId() != null ? config.getRegionId() : "ap-guangzhou");

        String url = "https://" + resolvedEndpoint;
        String responseBody = httpClient.postJson(url, jsonBody, headers);

        // Parse response
        Map<String, Object> fullResponse = CloudApiHttpClient.parseJson(responseBody);
        @SuppressWarnings("unchecked")
        Map<String, Object> response = (Map<String, Object>) fullResponse.get("Response");
        if (response == null) {
            throw new CloudApiException("Tencent Cloud API returned no Response field");
        }

        // Check for error
        @SuppressWarnings("unchecked")
        Map<String, Object> error = (Map<String, Object>) response.get("Error");
        if (error != null) {
            String code = CloudApiHttpClient.getString(error, "Code");
            String message = CloudApiHttpClient.getString(error, "Message");
            throw new CloudApiException(
                String.format("Tencent Cloud API error: code=%s, message=%s, action=%s",
                    code, message, action));
        }

        return response;
    }

    /**
     * Get the resolved API endpoint.
     */
    public String getResolvedEndpoint() {
        return resolvedEndpoint;
    }

    /**
     * Get the HTTP client (for use by TencentCloudMetadataProvider).
     */
    CloudApiHttpClient getHttpClient() {
        return httpClient;
    }
}