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
package org.apache.rocketmq.dashboard.service.client;

import apache.rocketmq.proxy.admin.v1.ClientDetail;
import apache.rocketmq.proxy.admin.v1.ClientInstance;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.apache.rocketmq.dashboard.model.SubscriptionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC channel client collector for RIP-1 CLIENT-01 dual-protocol unified view.
 *
 * <p>Collects client instance data from the gRPC Proxy Admin service (RIP-2 M1).
 * Supports both single-proxy ({@link ProxyAdminGrpcClient}) and multi-proxy
 * ({@link MultiProxyAdminClient}) modes for aggregated client data collection.
 *
 * <p>Uses the configured client to call ProxyClientAdminService RPCs:
 * <ul>
 *   <li>ListClients — paginated listing of all online gRPC clients</li>
 *   <li>DescribeClient — detailed info for a specific client</li>
 *   <li>ListClientsByGroup — filter by consumer group</li>
 *   <li>ListClientsByTopic — filter by topic</li>
 * </ul>
 *
 * <p>Converts proto-generated {@link ClientInstance} and {@link ClientDetail}
 * to the Dashboard's {@link org.apache.rocketmq.dashboard.model.ClientInstance} model,
 * enabling seamless integration with {@link org.apache.rocketmq.dashboard.service.impl.UnifiedClientServiceImpl}.</p>
 *
 * <p>Graceful degradation: if the gRPC channel is unavailable, all methods return
 * empty results, and the system falls back to Remoting-only client data.</p>
 */
public class GrpcClientCollector {

    private static final Logger log = LoggerFactory.getLogger(GrpcClientCollector.class);

    private final ProxyAdminGrpcClient grpcClient;
    private volatile MultiProxyAdminClient multiProxyClient;

    /**
     * Create a new GrpcClientCollector with a single gRPC client.
     *
     * @param grpcClient the ProxyAdminGrpcClient for gRPC calls
     */
    public GrpcClientCollector(ProxyAdminGrpcClient grpcClient) {
        this.grpcClient = grpcClient;
    }

    /**
     * Create a new GrpcClientCollector with a multi-proxy aggregation client.
     *
     * @param multiProxyClient the MultiProxyAdminClient for aggregated gRPC calls
     */
    public GrpcClientCollector(MultiProxyAdminClient multiProxyClient) {
        this.multiProxyClient = multiProxyClient;
        this.grpcClient = multiProxyClient.getClients().isEmpty() ? null
            : multiProxyClient.getClients().get(0);
    }

    /**
     * Set the multi-proxy client for aggregated queries.
     * When set, all list operations will fan-out to all proxies.
     */
    public void setMultiProxyClient(MultiProxyAdminClient multiProxyClient) {
        this.multiProxyClient = multiProxyClient;
    }

    /**
     * Get the underlying MultiProxyAdminClient.
     */
    public MultiProxyAdminClient getMultiProxyClient() {
        return multiProxyClient;
    }

    /**
     * List online client instances via gRPC Proxy Admin.
     * RIP-2 integration: calls ProxyClientAdminService.ListClients RPC.
     *
     * @param topic optional topic filter
     * @param group optional consumer group filter
     * @return list of Dashboard ClientInstance model objects
     */
    public List<org.apache.rocketmq.dashboard.model.ClientInstance> listClientInstances(
            Optional<String> topic, Optional<String> group) {
        // Use multi-proxy client if available for aggregated results
        if (multiProxyClient != null && multiProxyClient.isAvailable()) {
            return listClientInstancesMultiProxy(topic, group);
        }

        if (grpcClient == null || !grpcClient.isAvailable()) {
            log.debug("[GRPC-CLIENT] gRPC channel not available, returning empty list");
            return Collections.emptyList();
        }

        try {
            String topicFilter = topic.orElse(null);
            String groupFilter = group.orElse(null);

            List<ClientInstance> protoClients = grpcClient.listClients(
                groupFilter, topicFilter, null, 1, 100);

            List<org.apache.rocketmq.dashboard.model.ClientInstance> result = new ArrayList<>();
            for (ClientInstance proto : protoClients) {
                result.add(convertToModel(proto));
            }

            log.info("[GRPC-CLIENT] Listed {} gRPC clients (topic={}, group={})",
                result.size(), topicFilter, groupFilter);
            return result;
        } catch (Exception e) {
            log.error("[GRPC-CLIENT] Failed to list clients via gRPC", e);
            return Collections.emptyList();
        }
    }

    /**
     * List client instances using multi-proxy aggregation.
     */
    private List<org.apache.rocketmq.dashboard.model.ClientInstance> listClientInstancesMultiProxy(
            Optional<String> topic, Optional<String> group) {
        try {
            String topicFilter = topic.orElse(null);
            String groupFilter = group.orElse(null);

            List<ClientInstance> protoClients = multiProxyClient.listClients(
                groupFilter, topicFilter, null, 1, 100);

            List<org.apache.rocketmq.dashboard.model.ClientInstance> result = new ArrayList<>();
            for (ClientInstance proto : protoClients) {
                result.add(convertToModel(proto));
            }

            log.info("[GRPC-CLIENT] Listed {} gRPC clients from {} proxies (topic={}, group={})",
                result.size(), multiProxyClient.getTotalCount(), topicFilter, groupFilter);
            return result;
        } catch (Exception e) {
            log.error("[GRPC-CLIENT] Failed to list clients via multi-proxy", e);
            return Collections.emptyList();
        }
    }

    /**
     * Query detailed information for a specific client instance via gRPC Proxy Admin.
     * RIP-2 integration: calls ProxyClientAdminService.DescribeClient RPC.
     *
     * @param clientId the unique identifier of the client
     * @return Optional containing the ClientInstance if found, empty otherwise
     */
    public Optional<org.apache.rocketmq.dashboard.model.ClientInstance> getClientInstance(String clientId) {
        if (clientId == null || clientId.isEmpty()) {
            return Optional.empty();
        }

        try {
            // Use multi-proxy client if available
            ClientDetail detail = null;
            if (multiProxyClient != null && multiProxyClient.isAvailable()) {
                detail = multiProxyClient.describeClient(clientId);
            } else if (grpcClient != null && grpcClient.isAvailable()) {
                detail = grpcClient.describeClient(clientId);
            } else {
                return Optional.empty();
            }

            if (detail == null || !detail.hasClientInstance()) {
                log.debug("[GRPC-CLIENT] Client {} not found via gRPC", clientId);
                return Optional.empty();
            }

            org.apache.rocketmq.dashboard.model.ClientInstance model =
                convertDetailToModel(detail);
            log.debug("[GRPC-CLIENT] Described client {} via gRPC", clientId);
            return Optional.of(model);
        } catch (Exception e) {
            log.error("[GRPC-CLIENT] Failed to describe client {} via gRPC", clientId, e);
            return Optional.empty();
        }
    }

    /**
     * Retrieve subscription info for a specific client via gRPC Proxy Admin.
     * Extracts subscription data from the DescribeClient response.
     *
     * @param clientId the unique identifier of the client
     * @return list of subscription info entries
     */
    public List<SubscriptionInfo> getClientSubscriptions(String clientId) {
        if (clientId == null || clientId.isEmpty()) {
            return Collections.emptyList();
        }

        try {
            // Use multi-proxy client if available
            ClientDetail detail = null;
            if (multiProxyClient != null && multiProxyClient.isAvailable()) {
                detail = multiProxyClient.describeClient(clientId);
            } else if (grpcClient != null && grpcClient.isAvailable()) {
                detail = grpcClient.describeClient(clientId);
            } else {
                return Collections.emptyList();
            }

            if (detail == null || !detail.hasSettings()) {
                return Collections.emptyList();
            }

            List<SubscriptionInfo> subscriptions = new ArrayList<>();
            apache.rocketmq.proxy.admin.v1.ClientSettings settings = detail.getSettings();

            for (String topic : settings.getSubscriptionTopicsList()) {
                SubscriptionInfo info = new SubscriptionInfo();
                info.setTopic(topic);
                info.setSubExpression("*");
                subscriptions.add(info);
            }

            log.debug("[GRPC-CLIENT] Got {} subscriptions for client {} via gRPC",
                subscriptions.size(), clientId);
            return subscriptions;
        } catch (Exception e) {
            log.error("[GRPC-CLIENT] Failed to get subscriptions for client {} via gRPC", clientId, e);
            return Collections.emptyList();
        }
    }

    /**
     * Check whether the gRPC channel is available for client data collection.
     */
    public boolean hasDataAvailable() {
        if (multiProxyClient != null) {
            return multiProxyClient.isAvailable();
        }
        return grpcClient != null && grpcClient.isAvailable();
    }

    // ==================== Proto-to-Model Conversion ====================

    /**
     * Convert a proto ClientInstance to the Dashboard model.
     */
    private org.apache.rocketmq.dashboard.model.ClientInstance convertToModel(
            ClientInstance proto) {
        org.apache.rocketmq.dashboard.model.ClientInstance model =
            new org.apache.rocketmq.dashboard.model.ClientInstance();

        model.setClientId(proto.getClientId());
        model.setProtocolType(org.apache.rocketmq.dashboard.model.ClientInstance.ProtocolType.GRPC);
        model.setLanguage(proto.getLanguage().name());
        model.setSdkVersion(proto.getClientVersion());
        model.setClientAddress(proto.getAccessPoint());
        model.setConnectTime(new Date(proto.getConnectAt()));
        model.setActive(true);
        model.setLastHeartbeatTime(new Date(proto.getLastActiveAt()));

        // Map role
        switch (proto.getRole()) {
            case CLIENT_ROLE_PRODUCER:
                model.setClientType(org.apache.rocketmq.dashboard.model.ClientInstance.ClientType.PRODUCER);
                break;
            case CLIENT_ROLE_PUSH_CONSUMER:
                model.setClientType(org.apache.rocketmq.dashboard.model.ClientInstance.ClientType.PUSH_CONSUMER);
                break;
            case CLIENT_ROLE_SIMPLE_CONSUMER:
                model.setClientType(org.apache.rocketmq.dashboard.model.ClientInstance.ClientType.SIMPLE_CONSUMER);
                break;
            default:
                model.setClientType(org.apache.rocketmq.dashboard.model.ClientInstance.ClientType.PRODUCER);
                break;
        }

        // Set group and topics
        if (proto.getGroup() != null && !proto.getGroup().isEmpty()) {
            model.setConsumerGroup(proto.getGroup());
        }
        if (proto.getTopicsList() != null && !proto.getTopicsList().isEmpty()) {
            model.setTopics(new ArrayList<>(proto.getTopicsList()));
        }

        // gRPC-specific fields
        model.setEndpoint(proto.getAccessPoint());
        model.setLongConnectionActive(true);

        return model;
    }

    /**
     * Convert a proto ClientDetail (from DescribeClient) to the Dashboard model.
     */
    private org.apache.rocketmq.dashboard.model.ClientInstance convertDetailToModel(
            ClientDetail detail) {
        if (!detail.hasClientInstance()) {
            return null;
        }

        ClientInstance proto = detail.getClientInstance();
        org.apache.rocketmq.dashboard.model.ClientInstance model = convertToModel(proto);

        // Enrich with detail-specific fields
        if (detail.hasSettings()) {
            apache.rocketmq.proxy.admin.v1.ClientSettings settings = detail.getSettings();
            model.setPopEnabled(settings.getFifo() ? Boolean.FALSE : Boolean.TRUE);
            model.setSettingsVersion(String.valueOf(settings.getReceiveBatchSize()));

            // Build subscriptions from settings
            List<SubscriptionInfo> subscriptions = new ArrayList<>();
            for (String topic : settings.getSubscriptionTopicsList()) {
                SubscriptionInfo info = new SubscriptionInfo();
                info.setTopic(topic);
                info.setSubExpression("*");
                subscriptions.add(info);
            }
            model.setSubscriptions(subscriptions);
        }

        // Network info
        if (detail.hasNetworkInfo()) {
            apache.rocketmq.proxy.admin.v1.NetworkInfo networkInfo = detail.getNetworkInfo();
            model.setEndpoint(networkInfo.getRemoteAddress());
        }

        // Auth status
        if (detail.hasAuthStatus()) {
            apache.rocketmq.proxy.admin.v1.AuthStatus authStatus = detail.getAuthStatus();
            model.setActive(authStatus.getAuthenticated());
        }

        return model;
    }
}
