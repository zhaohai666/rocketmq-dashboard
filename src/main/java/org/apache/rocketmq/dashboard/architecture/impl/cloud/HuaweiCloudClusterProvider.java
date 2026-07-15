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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Huawei Cloud (DMS) RocketMQ cluster provider implementation.
 *
 * <p>Huawei Cloud DMS (Distributed Message Service) for RocketMQ provides
 * a managed RocketMQ service with RESTful API access.</p>
 *
 * <h3>Huawei Cloud DMS API Mapping</h3>
 * <ul>
 *   <li>ShowInstance → Cluster topology discovery</li>
 *   <li>ListInstances → Instance listing</li>
 *   <li>ListTopics → Topic listing</li>
 *   <li>CreateTopic → Topic creation</li>
 *   <li>DeleteTopic → Topic deletion</li>
 *   <li>ListGroups → Consumer group listing</li>
 *   <li>CreateGroup → Consumer group creation</li>
 * </ul>
 *
 * <h3>AK/SK Signing (SDK-HMAC-SHA256)</h3>
 * <p>Uses Huawei Cloud API Gateway signing (similar to AWS SigV4) with
 * SDK-HMAC-SHA256 algorithm for request authentication.</p>
 *
 * <p>Per RIP-1 ARCH-01 §5.1 M1, this adapter enables unified multi-cluster
 * management for Huawei Cloud-hosted RocketMQ instances.</p>
 */
public class HuaweiCloudClusterProvider extends AbstractCloudClusterProvider {

    private static final Logger log = LoggerFactory.getLogger(HuaweiCloudClusterProvider.class);

    /** Huawei Cloud DMS API endpoint pattern. */
    private static final String DEFAULT_ENDPOINT_PATTERN = "dms.%s.myhuaweicloud.com";

    /** Huawei Cloud DMS service name for signing. */
    private static final String SERVICE = "dms";

    /** Huawei Cloud DMS RocketMQ API version prefix. */
    private static final String API_PATH_PREFIX = "/v2/%s/instances";

    /** Resolved API endpoint host. */
    private String resolvedEndpoint;

    /** Project ID (extracted from extendedConfig or regionId). */
    private String projectId;

    /** Shared HTTP client for API calls. */
    private CloudApiHttpClient httpClient;

    public HuaweiCloudClusterProvider(CloudProviderConfig config) {
        super(config);
        log.info("HuaweiCloudClusterProvider created for instance: {}", config.getInstanceId());
    }

    @Override
    protected void doInitialize() throws Exception {
        log.info("Initializing Huawei Cloud DMS client for region: {}", config.getRegionId());
        this.httpClient = new CloudApiHttpClient();

        this.resolvedEndpoint = config.getEndpoint() != null && !config.getEndpoint().trim().isEmpty()
            ? config.getEndpoint()
            : String.format(DEFAULT_ENDPOINT_PATTERN, config.getRegionId());

        // Project ID is required for Huawei Cloud API paths
        this.projectId = config.getExtendedConfig() != null
            ? config.getExtendedConfig().getOrDefault("projectId", config.getRegionId())
            : config.getRegionId();

        // Validate credentials by calling ShowInstance
        try {
            Map<String, Object> result = callApi("GET",
                buildInstancePath(config.getInstanceId()), null, null);
            String instanceName = CloudApiHttpClient.getString(result, "name");
            log.info("Huawei Cloud DMS client initialized: instance={}, name={}, region={}, endpoint={}",
                config.getInstanceId(), instanceName, config.getRegionId(), resolvedEndpoint);
        } catch (CloudApiException e) {
            log.warn("Huawei Cloud credential validation returned error (may still work): {}", e.getMessage());
            log.info("Huawei Cloud DMS client initialized (with warning): instance={}, region={}, endpoint={}",
                config.getInstanceId(), config.getRegionId(), resolvedEndpoint);
        }
    }

    @Override
    protected void doShutdown() {
        log.info("Shutting down Huawei Cloud DMS client for instance: {}", config.getInstanceId());
    }

    @Override
    protected ClusterTopology doDiscoverTopology() throws Exception {
        log.info("Discovering topology for Huawei Cloud instance: {}", config.getInstanceId());
        ClusterTopology topology = new ClusterTopology();
        topology.setClusterName(config.getInstanceId());

        try {
            Map<String, Object> result = callApi("GET",
                buildInstancePath(config.getInstanceId()), null, null);

            String instanceName = CloudApiHttpClient.getString(result, "name");
            if (instanceName != null) {
                topology.setClusterName(instanceName);
            }

            String connectAddress = CloudApiHttpClient.getString(result, "connect_address");
            String nodeAddr = connectAddress != null ? connectAddress : resolvedEndpoint;
            topology.addNode(config.getInstanceId(), 0L, nodeAddr, "BROKER");

            if (connectAddress != null) {
                topology.getNamesrvAddresses().add(connectAddress);
            }

            // Check status
            String status = CloudApiHttpClient.getString(result, "status");
            if (!topology.getBrokerNodes().isEmpty()) {
                // Huawei DMS status: RUNNING, CREATING, FROZEN, etc.
                String nodeStatus = "RUNNING".equalsIgnoreCase(status) ? "ONLINE" : "OFFLINE";
                topology.getBrokerNodes().get(0).setStatus(nodeStatus);
            }

            // Extract namesrv addresses if available
            String namesrvAddress = CloudApiHttpClient.getString(result, "namesrv_address");
            if (namesrvAddress != null) {
                topology.getNamesrvAddresses().add(namesrvAddress);
                topology.addNode("namesrv-1", 0L, namesrvAddress, "PROXY");
            }

            log.info("Huawei Cloud topology discovered: instance={}, status={}", instanceName, status);
        } catch (CloudApiException e) {
            log.error("Failed to discover Huawei Cloud topology for instance {}: {}",
                config.getInstanceId(), e.getMessage());
            topology.addNode(config.getInstanceId(), 0L, resolvedEndpoint, "BROKER");
        }

        return topology;
    }

    @Override
    protected ClusterCapability doDetectCapability() throws Exception {
        log.info("Detecting capability for Huawei Cloud instance: {}", config.getInstanceId());
        ClusterCapability capability = new ClusterCapability();
        capability.setNamespaceSupported(true);
        capability.setLiteTopicSupported(false);
        capability.setPopConsumeSupported(true);
        capability.setAclV2Supported(true);
        capability.setGrpcClientSupported(false);
        capability.setDelayMessageSupported(true);
        capability.setTransactionMessageSupported(true);
        capability.setFifoMessageSupported(true);
        capability.setArchitectureVersion("5.x-cloud");
        capability.setRocketmqVersion("5.x-huawei");
        capability.setExtendedCapabilities(new HashSet<>());
        capability.getExtendedCapabilities().add("topicTypeValidation");
        capability.getExtendedCapabilities().add("metricsQuery");
        capability.getExtendedCapabilities().add("clientTrace");

        // Try to detect actual version from instance info
        try {
            Map<String, Object> result = callApi("GET",
                buildInstancePath(config.getInstanceId()), null, null);
            String engineVersion = CloudApiHttpClient.getString(result, "engine_version");
            if (engineVersion != null && !engineVersion.isEmpty()) {
                capability.setRocketmqVersion(engineVersion + "-huawei");
            }
        } catch (CloudApiException e) {
            log.debug("Could not detect Huawei Cloud version, using defaults: {}", e.getMessage());
        }

        return capability;
    }

    @Override
    protected List<String> doListNodeIds() throws Exception {
        log.info("Listing node IDs for Huawei Cloud instance: {}", config.getInstanceId());
        List<String> nodeIds = new ArrayList<>();
        nodeIds.add(config.getInstanceId());

        try {
            Map<String, Object> result = callApi("GET",
                buildInstancePath(config.getInstanceId()), null, null);
            String connectAddress = CloudApiHttpClient.getString(result, "connect_address");
            if (connectAddress != null) {
                nodeIds.add("endpoint:" + connectAddress);
            }
        } catch (CloudApiException e) {
            log.warn("Failed to list Huawei Cloud nodes: {}", e.getMessage());
        }

        return nodeIds;
    }

    @Override
    protected boolean doHealthCheck() throws Exception {
        log.info("Health check for Huawei Cloud instance: {}", config.getInstanceId());
        try {
            Map<String, Object> result = callApi("GET",
                buildInstancePath(config.getInstanceId()), null, null);
            String status = CloudApiHttpClient.getString(result, "status");
            boolean healthy = "RUNNING".equalsIgnoreCase(status);
            log.debug("Huawei Cloud health check: instance={}, status={}, healthy={}",
                config.getInstanceId(), status, healthy);
            return healthy;
        } catch (CloudApiException e) {
            log.error("Huawei Cloud health check failed: {}", e.getMessage());
            return false;
        }
    }

    // ==================== Huawei Cloud SDK-HMAC-SHA256 API Signing ====================

    /**
     * Call a Huawei Cloud DMS API using SDK-HMAC-SHA256 signing.
     *
     * @param method HTTP method (GET, POST, PUT, DELETE)
     * @param path   API path (e.g., /v2/{project_id}/instances/{instance_id})
     * @param queryParams query parameters (nullable)
     * @param body   request body for POST/PUT (nullable)
     * @return parsed JSON response
     */
    Map<String, Object> callApi(String method, String path, Map<String, String> queryParams,
                                String body) throws CloudApiException {
        Instant now = Instant.now();
        String xSdkDate = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
            .withZone(ZoneOffset.UTC).format(now);
        String dateStamp = DateTimeFormatter.ofPattern("yyyyMMdd")
            .withZone(ZoneOffset.UTC).format(now);

        String contentType = (body != null && !body.isEmpty()) ? "application/json" : "";
        String hashedPayload = CloudApiHttpClient.sha256Hex(body != null ? body : "");

        // Build canonical query string
        String canonicalQueryString = "";
        if (queryParams != null && !queryParams.isEmpty()) {
            TreeMap<String, String> sorted = new TreeMap<>(queryParams);
            canonicalQueryString = CloudApiHttpClient.buildQueryString(sorted);
        }

        // Build canonical headers (sorted by header name)
        TreeMap<String, String> headerMap = new TreeMap<>();
        headerMap.put("host", resolvedEndpoint);
        headerMap.put("x-sdk-date", xSdkDate);
        if (!contentType.isEmpty()) {
            headerMap.put("content-type", contentType);
        }

        StringBuilder canonicalHeaders = new StringBuilder();
        StringBuilder signedHeaders = new StringBuilder();
        for (Map.Entry<String, String> entry : headerMap.entrySet()) {
            canonicalHeaders.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
            if (signedHeaders.length() > 0) {
                signedHeaders.append(";");
            }
            signedHeaders.append(entry.getKey());
        }

        // Build Canonical Request
        String canonicalRequest = method.toUpperCase() + "\n"
            + path + "\n"
            + canonicalQueryString + "\n"
            + canonicalHeaders + "\n"
            + signedHeaders + "\n"
            + hashedPayload;

        // Build String to Sign
        String credentialScope = dateStamp + "/" + config.getRegionId() + "/" + SERVICE + "/sdk_request";
        String hashedCanonicalRequest = CloudApiHttpClient.sha256Hex(canonicalRequest);
        String stringToSign = "SDK-HMAC-SHA256\n"
            + xSdkDate + "\n"
            + credentialScope + "\n"
            + hashedCanonicalRequest;

        // Calculate Signature
        byte[] kDate = CloudApiHttpClient.hmacSha256(
            config.getSecretKey().getBytes(StandardCharsets.UTF_8), dateStamp);
        byte[] kRegion = CloudApiHttpClient.hmacSha256(kDate, config.getRegionId());
        byte[] kService = CloudApiHttpClient.hmacSha256(kRegion, SERVICE);
        byte[] kSigning = CloudApiHttpClient.hmacSha256(kService, "sdk_request");
        String signature = CloudApiHttpClient.hmacSha256Hex(kSigning, stringToSign);

        // Build Authorization header
        String authorization = "SDK-HMAC-SHA256"
            + " Credential=" + config.getAccessKey() + "/" + credentialScope
            + ", SignedHeaders=" + signedHeaders
            + ", Signature=" + signature;

        // Execute request
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Authorization", authorization);
        headers.put("Host", resolvedEndpoint);
        headers.put("X-Sdk-Date", xSdkDate);
        if (!contentType.isEmpty()) {
            headers.put("Content-Type", contentType);
        }

        String url = "https://" + resolvedEndpoint + path;
        if (canonicalQueryString != null && !canonicalQueryString.isEmpty()) {
            url += "?" + canonicalQueryString;
        }

        String responseBody;
        switch (method.toUpperCase()) {
            case "POST":
                responseBody = httpClient.postJson(url, body != null ? body : "", headers);
                break;
            case "PUT":
                responseBody = httpClient.putJson(url, body != null ? body : "", headers);
                break;
            case "DELETE":
                responseBody = httpClient.delete(url, headers);
                break;
            default:
                responseBody = httpClient.get(url, headers);
                break;
        }

        // Parse response
        Map<String, Object> result = CloudApiHttpClient.parseJson(responseBody);

        // Check for error
        String errorCode = CloudApiHttpClient.getString(result, "error_code");
        if (errorCode != null && !errorCode.isEmpty()) {
            String errorMsg = CloudApiHttpClient.getString(result, "error_msg");
            throw new CloudApiException(
                String.format("Huawei Cloud API error: code=%s, msg=%s, path=%s",
                    errorCode, errorMsg, path));
        }

        return result;
    }

    // ==================== Path Helpers ====================

    private String buildInstancePath(String instanceId) {
        return String.format(API_PATH_PREFIX, projectId) + "/" + instanceId;
    }

    String buildInstancesBasePath() {
        return String.format(API_PATH_PREFIX, projectId);
    }

    /**
     * Get the resolved API endpoint.
     */
    public String getResolvedEndpoint() {
        return resolvedEndpoint;
    }

    /**
     * Get the project ID.
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * Get the HTTP client (for use by HuaweiCloudMetadataProvider).
     */
    CloudApiHttpClient getHttpClient() {
        return httpClient;
    }
}