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

import org.apache.rocketmq.dashboard.model.CloudProviderConfig;
import org.apache.rocketmq.dashboard.model.ClusterCapability;
import org.apache.rocketmq.dashboard.model.ClusterTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * AWS (Amazon MQ for RocketMQ) cloud provider SPI plugin placeholder.
 *
 * <p>This is a reference implementation for the {@link CloudProviderFactory.CloudProviderPlugin}
 * SPI interface, demonstrating how third-party cloud providers can be integrated
 * via Java ServiceLoader mechanism.</p>
 *
 * <h3>Integration Guide</h3>
 * <p>To enable this plugin:</p>
 * <ol>
 *   <li>Add AWS SDK dependency to pom.xml</li>
 *   <li>Implement the actual AWS API calls in {@link #doInitialize()}, etc.</li>
 *   <li>Create META-INF/services/org.apache.rocketmq.dashboard.architecture.impl.cloud.CloudProviderFactory$CloudProviderPlugin
 *       with the fully qualified class name of {@link AwsCloudProviderPlugin}</li>
 * </ol>
 *
 * <h3>AWS MQ API Mapping (future)</h3>
 * <ul>
 *   <li>DescribeBroker → Cluster topology discovery</li>
 *   <li>ListConfigurations → Configuration listing</li>
 *   <li>CreateBroker → Instance creation</li>
 *   <li>DeleteBroker → Instance deletion</li>
 * </ul>
 *
 * <p>Per RIP-1 ARCH-01 §5.1, third-party cloud providers are supported via
 * the CloudProviderPlugin SPI interface and ServiceLoader discovery.</p>
 */
public class AwsCloudProviderPlugin implements CloudProviderFactory.CloudProviderPlugin {

    private static final Logger log = LoggerFactory.getLogger(AwsCloudProviderPlugin.class);

    /** AWS cloud provider type identifier. */
    public static final String PROVIDER_TYPE = "cloud-aws";

    @Override
    public String getProviderType() {
        return PROVIDER_TYPE;
    }

    @Override
    public CloudProviderFactory.CloudProviderBundle createBundle(CloudProviderConfig config) {
        log.info("Creating AWS cloud provider bundle for instance: {}", config.getInstanceId());
        AwsClusterProvider clusterProvider = new AwsClusterProvider(config);
        AwsMetadataProvider metadataProvider = new AwsMetadataProvider(config);
        CloudAdminClient adminClient = new CloudAdminClient(config);
        return new CloudProviderFactory.CloudProviderBundle(clusterProvider, metadataProvider, adminClient, config);
    }

    // ==================== AWS Cluster Provider ====================

    /**
     * AWS RocketMQ cluster provider (SPI placeholder).
     *
     * <p>Awaiting community contribution for actual AWS SDK integration.
     * When AWS provides a managed RocketMQ service, implement the API calls here.</p>
     */
    static class AwsClusterProvider extends AbstractCloudClusterProvider {

        private static final Logger log = LoggerFactory.getLogger(AwsClusterProvider.class);

        AwsClusterProvider(CloudProviderConfig config) {
            super(config);
            log.info("AwsClusterProvider created for instance: {}", config.getInstanceId());
        }

        @Override
        protected void doInitialize() throws Exception {
            log.info("[SPI-STUB] Initializing AWS cloud provider for region: {}", config.getRegionId());
            // TODO: AWS SDK integration
            // AmazonMQClient client = AmazonMQClientBuilder.standard()
            //     .withCredentials(new AWSStaticCredentialsProvider(
            //         new BasicAWSCredentials(config.getAccessKey(), config.getSecretKey())))
            //     .withRegion(config.getRegionId())
            //     .build();
            log.warn("AWS cloud provider is SPI placeholder - actual SDK integration pending community contribution");
        }

        @Override
        protected void doShutdown() {
            log.info("[SPI-STUB] Shutting down AWS cloud provider for instance: {}", config.getInstanceId());
        }

        @Override
        protected ClusterTopology doDiscoverTopology() throws Exception {
            log.info("[SPI-STUB] Discovering topology for AWS instance: {}", config.getInstanceId());
            ClusterTopology topology = new ClusterTopology();
            topology.setClusterName(config.getInstanceId());
            topology.addNode(config.getInstanceId(), 0L,
                config.getEndpoint() != null ? config.getEndpoint() : "localhost", "BROKER");
            return topology;
        }

        @Override
        protected ClusterCapability doDetectCapability() throws Exception {
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
            capability.setRocketmqVersion("5.x-aws");
            capability.setExtendedCapabilities(new HashSet<>());
            return capability;
        }

        @Override
        protected List<String> doListNodeIds() throws Exception {
            return Collections.singletonList(config.getInstanceId());
        }

        @Override
        protected boolean doHealthCheck() throws Exception {
            log.info("[SPI-STUB] Health check for AWS instance: {}", config.getInstanceId());
            return true;
        }
    }

    // ==================== AWS Metadata Provider ====================

    /**
     * AWS RocketMQ metadata provider (SPI placeholder).
     */
    static class AwsMetadataProvider extends AbstractCloudMetadataProvider {

        private static final Logger log = LoggerFactory.getLogger(AwsMetadataProvider.class);

        AwsMetadataProvider(CloudProviderConfig config) {
            super(config);
        }

        @Override
        protected List<org.apache.rocketmq.dashboard.model.NamespaceInfo> doListNamespaces() throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected java.util.Optional<org.apache.rocketmq.dashboard.model.NamespaceInfo> doGetNamespace(String namespace) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected void doCreateNamespace(org.apache.rocketmq.dashboard.model.NamespaceInfo namespace) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected void doUpdateNamespace(org.apache.rocketmq.dashboard.model.NamespaceInfo namespace) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected void doDeleteNamespace(String namespace) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected List<org.apache.rocketmq.dashboard.model.TopicInfo> doListTopics(java.util.Optional<String> namespace) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected java.util.Optional<org.apache.rocketmq.dashboard.model.TopicInfo> doGetTopic(String topic, java.util.Optional<String> namespace) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected void doCreateTopic(org.apache.rocketmq.dashboard.model.TopicInfo topic) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected void doUpdateTopic(org.apache.rocketmq.dashboard.model.TopicInfo topic) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected void doDeleteTopic(String topic, java.util.Optional<String> namespace) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected boolean doValidateTopicType(String topic, org.apache.rocketmq.dashboard.model.TopicType expectedType) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected List<org.apache.rocketmq.dashboard.model.ConsumerGroupInfo> doListConsumerGroups(java.util.Optional<String> namespace) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected java.util.Optional<org.apache.rocketmq.dashboard.model.ConsumerGroupInfo> doGetConsumerGroup(String consumerGroup, java.util.Optional<String> namespace) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected void doCreateConsumerGroup(org.apache.rocketmq.dashboard.model.ConsumerGroupInfo consumerGroup) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected void doUpdateConsumerGroup(org.apache.rocketmq.dashboard.model.ConsumerGroupInfo consumerGroup) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected void doDeleteConsumerGroup(String consumerGroup, java.util.Optional<String> namespace) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected List<org.apache.rocketmq.dashboard.model.SubscriptionInfo> doListSubscriptions(String groupName) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected void doResetConsumerGroupOffset(String groupName, String topic, long timestamp) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected List<org.apache.rocketmq.dashboard.model.ClientInstance> doListClientInstances(java.util.Optional<String> topic, java.util.Optional<String> group) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }

        @Override
        protected java.util.Optional<org.apache.rocketmq.dashboard.model.ClientInstance> doGetClientInstance(String clientId) throws Exception {
            throw new UnsupportedOperationException("AWS cloud provider is SPI placeholder - not implemented");
        }
    }
}
