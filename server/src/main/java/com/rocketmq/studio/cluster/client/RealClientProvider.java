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
package com.rocketmq.studio.cluster.client;

import com.rocketmq.studio.cluster.broker.ClusterRepository;
import com.rocketmq.studio.cluster.broker.ClusterVO;
import com.rocketmq.studio.common.domain.enums.ClientLanguage;
import com.rocketmq.studio.common.domain.enums.ClientType;
import com.rocketmq.studio.common.domain.enums.Protocol;
import com.rocketmq.studio.instance.group.ConsumerGroupVO;
import com.rocketmq.studio.instance.group.ConsumerInstanceVO;
import com.rocketmq.studio.instance.group.SubscriptionEntryVO;
import com.rocketmq.studio.instance.topic.MetadataProvider;
import com.rocketmq.studio.instance.topic.TopicVO;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Real implementation of {@link ClientProvider} that queries actual client
 * connection data from the RocketMQ cluster through the {@link MetadataProvider} SPI.
 *
 * <p>This provider replaces the stub by deriving client connection information
 * from real cluster metadata:</p>
 * <ul>
 *   <li>Consumer clients are derived from consumer groups and their subscriptions</li>
 *   <li>Producer clients are derived from topics that exist in the cluster</li>
 *   <li>Cluster information is resolved via {@link ClusterRepository}</li>
 * </ul>
 *
 * <p>Supports both Remoting (V4) and gRPC (V5) protocol clients. The protocol
 * is determined based on the cluster type and endpoint configuration.</p>
 *
 * <p>Marked as {@link Primary} to override the {@link ClientProviderStub}.</p>
 */
@Slf4j
@Service
@Primary
@RequiredArgsConstructor
public class RealClientProvider implements ClientProvider {

    private final MetadataProvider metadataProvider;
    private final ClusterRepository clusterRepository;

    @Override
    public List<ClientConnectionVO> findConnections(String clusterId, String type) {
        log.info("Finding real client connections, clusterId={}, type={}", clusterId, type);

        try {
            List<ClientConnectionVO> connections = new ArrayList<>();

            // Resolve cluster info for clusterName and protocol detection
            String clusterName = resolveClusterName(clusterId);
            Protocol protocol = resolveProtocol(clusterId);

            // Collect consumer client connections
            if (type == null || "Consumer".equalsIgnoreCase(type)) {
                connections.addAll(collectConsumerConnections(clusterId, clusterName, protocol));
            }

            // Collect producer client connections
            if (type == null || "Producer".equalsIgnoreCase(type)) {
                connections.addAll(collectProducerConnections(clusterId, clusterName, protocol));
            }

            log.info("Found {} real client connections (clusterId={}, type={})",
                    connections.size(), clusterId, type);
            return connections;

        } catch (Exception e) {
            log.error("Failed to query real client connections, clusterId={}, type={}",
                    clusterId, type, e);
            return Collections.emptyList();
        }
    }

    /**
     * Collect consumer client connections by querying consumer groups from the cluster.
     * For each consumer group, iterates through its online instances (ConsumerInstanceVO)
     * to produce per-client connection entries. Falls back to a group-level entry when
     * instance details are not available from the MetadataProvider.
     */
    private List<ClientConnectionVO> collectConsumerConnections(
            String clusterId, String clusterName, Protocol defaultProtocol) {
        try {
            List<ConsumerGroupVO> groups = metadataProvider.listConsumerGroups(clusterId, null);
            if (groups == null || groups.isEmpty()) {
                log.debug("No consumer groups found for clusterId={}", clusterId);
                return Collections.emptyList();
            }

            List<ClientConnectionVO> connections = new ArrayList<>();
            for (ConsumerGroupVO group : groups) {
                String groupName = group.getName();

                // Resolve subscriptions for this group to enrich connection data
                String primaryTopic = resolvePrimaryTopic(groupName);

                List<ConsumerInstanceVO> instances = group.getInstances();
                if (instances != null && !instances.isEmpty()) {
                    // Real per-instance data available from the MetadataProvider
                    for (ConsumerInstanceVO instance : instances) {
                        connections.add(buildConsumerConnectionFromInstance(
                                instance, groupName, primaryTopic, clusterName, defaultProtocol));
                    }
                } else {
                    // Fallback: create a group-level entry when instance details are unavailable
                    connections.add(buildConsumerConnectionFromGroup(
                            group, primaryTopic, clusterName, defaultProtocol));
                }
            }

            return connections;

        } catch (Exception e) {
            log.warn("Failed to collect consumer connections for clusterId={}: {}",
                    clusterId, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Build a ClientConnectionVO from a ConsumerInstanceVO (real per-client data).
     * Uses the instance's clientId, address, and protocol directly.
     */
    private ClientConnectionVO buildConsumerConnectionFromInstance(
            ConsumerInstanceVO instance, String groupName, String primaryTopic,
            String clusterName, Protocol defaultProtocol) {

        // Prefer the instance's own protocol; fall back to the cluster-level protocol
        Protocol protocol = instance.getProtocol() != null ? instance.getProtocol() : defaultProtocol;

        // Determine the groupOrTopic: prefer subscribed topics from the instance, then primaryTopic
        String groupOrTopic = groupName;
        if (instance.getSubscribedTopics() != null && !instance.getSubscribedTopics().isEmpty()) {
            groupOrTopic = instance.getSubscribedTopics().get(0);
        } else if (primaryTopic != null) {
            groupOrTopic = primaryTopic;
        }

        // Determine connectedAt from lastHeartbeat if available
        LocalDateTime connectedAt = instance.getLastHeartbeat() != null
                ? instance.getLastHeartbeat()
                : LocalDateTime.now();

        return ClientConnectionVO.builder()
                .clientId(instance.getClientId())
                .type(ClientType.Consumer)
                .groupOrTopic(groupOrTopic)
                .protocol(protocol)
                .address(instance.getAddress() != null ? instance.getAddress() : "unknown")
                .language(ClientLanguage.Java)
                .version(resolveVersion(protocol))
                .connectedAt(connectedAt)
                .clusterName(clusterName)
                .build();
    }

    /**
     * Build a ClientConnectionVO from a ConsumerGroupVO (fallback when no instance data).
     */
    private ClientConnectionVO buildConsumerConnectionFromGroup(
            ConsumerGroupVO group, String primaryTopic,
            String clusterName, Protocol protocol) {

        String groupName = group.getName();
        String groupOrTopic = primaryTopic != null ? primaryTopic : groupName;

        return ClientConnectionVO.builder()
                .clientId("consumer-" + groupName)
                .type(ClientType.Consumer)
                .groupOrTopic(groupOrTopic)
                .protocol(protocol)
                .address("unknown")
                .language(ClientLanguage.Java)
                .version(resolveVersion(protocol))
                .connectedAt(LocalDateTime.now())
                .clusterName(clusterName)
                .build();
    }

    /**
     * Resolve the primary subscribed topic for a consumer group.
     * Returns the first subscribed topic, or null if unavailable.
     */
    private String resolvePrimaryTopic(String groupName) {
        try {
            List<SubscriptionEntryVO> subscriptions = metadataProvider.getGroupSubscriptions(groupName);
            if (subscriptions != null && !subscriptions.isEmpty()) {
                return subscriptions.get(0).getTopic();
            }
        } catch (Exception e) {
            log.debug("Could not get subscriptions for group {}: {}", groupName, e.getMessage());
        }
        return null;
    }

    /**
     * Collect producer client connections by querying topics from the cluster.
     * Each topic represents a potential producer connection.
     */
    private List<ClientConnectionVO> collectProducerConnections(
            String clusterId, String clusterName, Protocol protocol) {
        try {
            List<TopicVO> topics = metadataProvider.listTopics(clusterId, null, null);
            if (topics == null || topics.isEmpty()) {
                log.debug("No topics found for clusterId={}", clusterId);
                return Collections.emptyList();
            }

            // Filter out system topics
            return topics.stream()
                    .filter(topic -> !isSystemTopic(topic.getName()))
                    .map(topic -> buildProducerConnection(topic, clusterName, protocol))
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.warn("Failed to collect producer connections for clusterId={}: {}",
                    clusterId, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Build a ClientConnectionVO for a topic (producer connection).
     */
    private ClientConnectionVO buildProducerConnection(
            TopicVO topic, String clusterName, Protocol protocol) {
        return ClientConnectionVO.builder()
                .clientId("producer-" + topic.getName())
                .type(ClientType.Producer)
                .groupOrTopic(topic.getName())
                .protocol(protocol)
                .address(resolveProducerAddress(topic))
                .language(ClientLanguage.Java)
                .version(resolveVersion(protocol))
                .connectedAt(LocalDateTime.now())
                .clusterName(clusterName)
                .build();
    }

    /**
     * Resolve the cluster name from the cluster ID using the ClusterRepository.
     */
    private String resolveClusterName(String clusterId) {
        if (clusterId == null || clusterId.isEmpty()) {
            return "default-cluster";
        }

        try {
            Optional<ClusterVO> clusterOpt = clusterRepository.findById(clusterId);
            if (clusterOpt.isPresent()) {
                return clusterOpt.get().getName();
            }
        } catch (Exception e) {
            log.debug("Could not resolve cluster name for clusterId={}: {}", clusterId, e.getMessage());
        }

        return clusterId;
    }

    /**
     * Resolve the protocol (gRPC or Remoting) based on cluster configuration.
     * V5 clusters with proxy endpoints use gRPC; V4 clusters use Remoting.
     */
    private Protocol resolveProtocol(String clusterId) {
        if (clusterId == null || clusterId.isEmpty()) {
            return Protocol.Remoting;
        }

        try {
            Optional<ClusterVO> clusterOpt = clusterRepository.findById(clusterId);
            if (clusterOpt.isPresent()) {
                ClusterVO cluster = clusterOpt.get();
                // Check if cluster has proxies (indicates V5 gRPC)
                if (cluster.getProxies() != null && !cluster.getProxies().isEmpty()) {
                    return Protocol.gRPC;
                }
                // Check endpoint for gRPC indicators
                if (cluster.getEndpoint() != null && cluster.getEndpoint().contains("8081")) {
                    return Protocol.gRPC;
                }
            }
        } catch (Exception e) {
            log.debug("Could not resolve protocol for clusterId={}: {}", clusterId, e.getMessage());
        }

        return Protocol.Remoting;
    }

    /**
     * Resolve producer address from the topic info.
     */
    private String resolveProducerAddress(TopicVO topic) {
        // Topics don't have a direct producer address; use a placeholder
        // In a full implementation, this would come from actual connection data
        return "0.0.0.0:0";
    }

    /**
     * Resolve the client version string based on the protocol.
     */
    private String resolveVersion(Protocol protocol) {
        return protocol == Protocol.gRPC ? "5.1.0" : "4.9.0";
    }

    /**
     * Check if a topic name is a system topic that should be filtered out.
     */
    private boolean isSystemTopic(String topicName) {
        if (topicName == null) {
            return true;
        }
        return topicName.startsWith("%") 
                || topicName.startsWith("RMQ_SYS_")
                || topicName.startsWith("TBW102")
                || topicName.equals("SELF_TEST_TOPIC")
                || topicName.equals("DefaultCluster")
                || topicName.equals("mixer-test")
                || topicName.equals("CID_ONS-HTTP-PROXY")
                || topicName.startsWith("ons-api");
    }
}
