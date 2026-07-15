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
import org.apache.rocketmq.dashboard.model.ClientInstance;
import org.apache.rocketmq.dashboard.model.CloudProviderConfig;
import org.apache.rocketmq.dashboard.model.ConsumerGroupInfo;
import org.apache.rocketmq.dashboard.model.MessageInfo;
import org.apache.rocketmq.dashboard.model.NamespaceInfo;
import org.apache.rocketmq.dashboard.model.SubscriptionInfo;
import org.apache.rocketmq.dashboard.model.TopicInfo;
import org.apache.rocketmq.dashboard.model.TopicType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Aliyun RocketMQ cloud metadata provider implementation.
 *
 * <p>Implements the {@link org.apache.rocketmq.dashboard.architecture.MetadataProvider} SPI
 * for Aliyun RocketMQ using the Aliyun OpenAPI with HMAC-SHA1 signature authentication.</p>
 *
 * <h3>Aliyun OpenAPI Mapping</h3>
 * <ul>
 *   <li>ListNamespaces -> OnsInstanceInServiceList</li>
 *   <li>ListTopics -> OnsTopicList</li>
 *   <li>CreateTopic -> OnsTopicCreate</li>
 *   <li>DeleteTopic -> OnsTopicDelete</li>
 *   <li>ListConsumerGroups -> OnsGroupList</li>
 *   <li>CreateConsumerGroup -> OnsGroupCreate</li>
 *   <li>DeleteConsumerGroup -> OnsGroupDelete</li>
 *   <li>QueryMessages -> OnsMessagePageQueryByTopic</li>
 *   <li>QueryMessageById -> OnsMessageGetByMsgId</li>
 *   <li>ConsumerStatus -> OnsConsumerStatus</li>
 *   <li>ConsumerResetOffset -> OnsConsumerResetOffset</li>
 * </ul>
 *
 * <p>Per RIP-1 META-01, this adapter enables unified metadata management
 * for Aliyun-hosted RocketMQ instances alongside self-hosted V4/V5 clusters.</p>
 */
public class AliyunMetadataProvider extends AbstractCloudMetadataProvider {

    private static final Logger log = LoggerFactory.getLogger(AliyunMetadataProvider.class);

    /** Aliyun RocketMQ API endpoint pattern. */
    private static final String DEFAULT_ENDPOINT_PATTERN = "rocketmq.%s.aliyuncs.com";

    /** Aliyun RocketMQ API version (5.0 serverless API). */
    private static final String API_VERSION = "2022-08-01";

    /** Aliyun API signature method. */
    private static final String SIGNATURE_METHOD = "HMAC-SHA1";

    /** Aliyun API signature version. */
    private static final String SIGNATURE_VERSION = "1.0";

    /** Resolved API endpoint. */
    private String resolvedEndpoint;

    /** Shared HTTP client for API calls. */
    private CloudApiHttpClient httpClient;

    public AliyunMetadataProvider(CloudProviderConfig config) {
        super(config);
        log.info("AliyunMetadataProvider created for instance: {}", config.getInstanceId());
    }

    /**
     * Initialize the HTTP client and resolve the API endpoint.
     */
    public void initialize() throws Exception {
        this.httpClient = new CloudApiHttpClient();

        if (config.getEndpoint() != null && !config.getEndpoint().trim().isEmpty()) {
            this.resolvedEndpoint = config.getEndpoint();
        } else {
            this.resolvedEndpoint = String.format(DEFAULT_ENDPOINT_PATTERN,
                config.getRegionId() != null ? config.getRegionId() : "cn-hangzhou");
        }

        log.info("AliyunMetadataProvider initialized: instance={}, region={}, endpoint={}",
            config.getInstanceId(), config.getRegionId(), resolvedEndpoint);
    }

    @Override
    public boolean supportsCapability(String capability) {
        switch (capability) {
            case "namespace":
            case "liteTopic":
            case "popConsume":
            case "aclV2":
            case "grpcClient":
                return true;
            case "remotingClient":
                return false;  // Aliyun 5.0 uses OpenAPI, not remoting
            default:
                return false;
        }
    }

    // ==================== Namespace Operations ====================

    @Override
    protected List<NamespaceInfo> doListNamespaces() throws Exception {
        log.info("Listing namespaces (instances) for Aliyun region: {}", config.getRegionId());

        try {
            Map<String, Object> result = callApi("OnsInstanceInServiceList", null);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> instanceList =
                (List<Map<String, Object>>) result.get("InstanceList");
            if (instanceList == null || instanceList.isEmpty()) {
                return Collections.emptyList();
            }

            List<NamespaceInfo> namespaces = new ArrayList<>();
            for (Map<String, Object> instance : instanceList) {
                NamespaceInfo ns = new NamespaceInfo();
                ns.setNamespaceName(CloudApiHttpClient.getString(instance, "InstanceId"));
                ns.setDisplayName(CloudApiHttpClient.getString(instance, "InstanceName"));
                ns.setStatus(mapInstanceStatus(
                    CloudApiHttpClient.getInt(instance, "InstanceStatus", -1)));

                Long createTime = (Long) instance.get("CreateTime");
                if (createTime != null) {
                    ns.setCreateTime(new Date(createTime));
                }

                Long updateTime = (Long) instance.get("UpdateTime");
                if (updateTime != null) {
                    ns.setUpdateTime(new Date(updateTime));
                }

                ns.setClusterName(config.getInstanceId());

                // Set quota config from instance info
                NamespaceInfo.QuotaConfig quota = new NamespaceInfo.QuotaConfig();
                quota.setMaxTopicCount(CloudApiHttpClient.getInt(instance, "MaxTopicCount", 1000));
                quota.setMaxConsumerGroupCount(CloudApiHttpClient.getInt(instance, "MaxGroupCount", 500));
                ns.setQuotaConfig(quota);

                namespaces.add(ns);
            }

            log.info("Listed {} Aliyun namespaces", namespaces.size());
            return namespaces;
        } catch (CloudApiException e) {
            log.error("Failed to list Aliyun namespaces: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    protected Optional<NamespaceInfo> doGetNamespace(String namespace) throws Exception {
        log.info("Getting namespace: {} for Aliyun instance: {}", namespace, config.getInstanceId());

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", namespace);
            Map<String, Object> result = callApi("OnsInstanceBaseInfo", params);

            @SuppressWarnings("unchecked")
            Map<String, Object> info = (Map<String, Object>) result.get("InstanceBaseInfo");
            if (info == null) {
                return Optional.empty();
            }

            NamespaceInfo ns = new NamespaceInfo();
            ns.setNamespaceName(CloudApiHttpClient.getString(info, "InstanceId"));
            ns.setDisplayName(CloudApiHttpClient.getString(info, "InstanceName"));
            ns.setStatus(mapInstanceStatus(
                CloudApiHttpClient.getInt(info, "InstanceStatus", -1)));
            ns.setClusterName(config.getInstanceId());

            Long createTime = (Long) info.get("CreateTime");
            if (createTime != null) {
                ns.setCreateTime(new Date(createTime));
            }

            return Optional.of(ns);
        } catch (CloudApiException e) {
            log.warn("Failed to get Aliyun namespace {}: {}", namespace, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    protected void doCreateNamespace(NamespaceInfo namespace) throws Exception {
        // Aliyun instances are created via the console, not via API in most cases.
        // The 5.0 API supports creating serverless instances.
        log.info("Creating namespace (instance): {} for Aliyun", namespace.getNamespaceName());
        throw new UnsupportedOperationException(
            "Aliyun RocketMQ instance creation is not supported via Dashboard API. "
            + "Please create instances through the Aliyun console or Terraform.");
    }

    @Override
    protected void doUpdateNamespace(NamespaceInfo namespace) throws Exception {
        log.info("Updating namespace: {} for Aliyun instance: {}",
            namespace.getNamespaceName(), config.getInstanceId());
        // Aliyun instance updates are limited; most properties are immutable after creation
        throw new UnsupportedOperationException(
            "Aliyun RocketMQ instance update is not supported via Dashboard API. "
            + "Please modify instance settings through the Aliyun console.");
    }

    @Override
    protected void doDeleteNamespace(String namespace) throws Exception {
        log.info("Deleting namespace (instance): {} for Aliyun", namespace);
        throw new UnsupportedOperationException(
            "Aliyun RocketMQ instance deletion is not supported via Dashboard API. "
            + "Please delete instances through the Aliyun console.");
    }

    // ==================== Topic Operations ====================

    @Override
    protected List<TopicInfo> doListTopics(Optional<String> namespace) throws Exception {
        log.info("Listing topics for Aliyun instance: {}, namespace: {}",
            config.getInstanceId(), namespace);

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            Map<String, Object> result = callApi("OnsTopicList", params);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> topicList =
                (List<Map<String, Object>>) result.get("Data");
            if (topicList == null || topicList.isEmpty()) {
                return Collections.emptyList();
            }

            List<TopicInfo> topics = new ArrayList<>();
            for (Map<String, Object> topicData : topicList) {
                TopicInfo topic = mapToTopicInfo(topicData);
                topics.add(topic);
            }

            log.info("Listed {} topics for Aliyun instance: {}", topics.size(), config.getInstanceId());
            return topics;
        } catch (CloudApiException e) {
            log.error("Failed to list topics for Aliyun instance {}: {}",
                config.getInstanceId(), e.getMessage());
            throw e;
        }
    }

    @Override
    protected Optional<TopicInfo> doGetTopic(String topic, Optional<String> namespace) throws Exception {
        log.info("Getting topic: {} for Aliyun instance: {}", topic, config.getInstanceId());

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            params.put("Topic", topic);
            Map<String, Object> result = callApi("OnsTopicStatus", params);

            @SuppressWarnings("unchecked")
            Map<String, Object> topicData = (Map<String, Object>) result.get("Data");
            if (topicData == null) {
                return Optional.empty();
            }

            return Optional.of(mapToTopicInfo(topicData));
        } catch (CloudApiException e) {
            log.warn("Failed to get topic {} for Aliyun instance {}: {}",
                topic, config.getInstanceId(), e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    protected void doCreateTopic(TopicInfo topic) throws Exception {
        log.info("Creating topic: {} for Aliyun instance: {}",
            topic.getTopicName(), config.getInstanceId());

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            params.put("Topic", topic.getTopicName());

            // Map topic type to Aliyun message type
            int messageType = mapTopicTypeToAliyunMessageType(topic.getTopicType());
            params.put("MessageType", String.valueOf(messageType));

            if (topic.getAttributes() != null) {
                String remark = topic.getAttributes().get("remark");
                if (remark != null) {
                    params.put("Remark", remark);
                }
            }

            callApi("OnsTopicCreate", params);
            log.info("Topic created successfully: {} for Aliyun instance: {}",
                topic.getTopicName(), config.getInstanceId());
        } catch (CloudApiException e) {
            log.error("Failed to create topic {} for Aliyun instance {}: {}",
                topic.getTopicName(), config.getInstanceId(), e.getMessage());
            throw e;
        }
    }

    @Override
    protected void doUpdateTopic(TopicInfo topic) throws Exception {
        log.info("Updating topic: {} for Aliyun instance: {}",
            topic.getTopicName(), config.getInstanceId());
        // Aliyun ONS API does not support direct topic update.
        // Topic properties like message type are immutable after creation.
        // Only the remark/description can be updated in some API versions.
        throw new UnsupportedOperationException(
            "Aliyun RocketMQ topic update is not supported via API. "
            + "Topic properties are immutable after creation. "
            + "Delete and recreate the topic to change its configuration.");
    }

    @Override
    protected void doDeleteTopic(String topic, Optional<String> namespace) throws Exception {
        log.info("Deleting topic: {} for Aliyun instance: {}", topic, config.getInstanceId());

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            params.put("Topic", topic);
            callApi("OnsTopicDelete", params);
            log.info("Topic deleted successfully: {} for Aliyun instance: {}",
                topic, config.getInstanceId());
        } catch (CloudApiException e) {
            log.error("Failed to delete topic {} for Aliyun instance {}: {}",
                topic, config.getInstanceId(), e.getMessage());
            throw e;
        }
    }

    @Override
    protected boolean doValidateTopicType(String topic, TopicType expectedType) throws Exception {
        log.info("Validating topic type: {} expected: {} for Aliyun instance: {}",
            topic, expectedType, config.getInstanceId());

        try {
            Optional<TopicInfo> existingTopic = doGetTopic(topic, Optional.empty());
            if (existingTopic.isEmpty()) {
                return true; // Topic doesn't exist, any type is valid
            }
            TopicInfo actual = existingTopic.get();
            return expectedType.equals(actual.getTopicType());
        } catch (Exception e) {
            log.warn("Failed to validate topic type for {}: {}", topic, e.getMessage());
            return false;
        }
    }

    // ==================== Consumer Group Operations ====================

    @Override
    protected List<ConsumerGroupInfo> doListConsumerGroups(Optional<String> namespace) throws Exception {
        log.info("Listing consumer groups for Aliyun instance: {}, namespace: {}",
            config.getInstanceId(), namespace);

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            Map<String, Object> result = callApi("OnsGroupList", params);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> groupList =
                (List<Map<String, Object>>) result.get("Data");
            if (groupList == null || groupList.isEmpty()) {
                return Collections.emptyList();
            }

            List<ConsumerGroupInfo> groups = new ArrayList<>();
            for (Map<String, Object> groupData : groupList) {
                ConsumerGroupInfo group = mapToConsumerGroupInfo(groupData);
                groups.add(group);
            }

            log.info("Listed {} consumer groups for Aliyun instance: {}",
                groups.size(), config.getInstanceId());
            return groups;
        } catch (CloudApiException e) {
            log.error("Failed to list consumer groups for Aliyun instance {}: {}",
                config.getInstanceId(), e.getMessage());
            throw e;
        }
    }

    @Override
    protected Optional<ConsumerGroupInfo> doGetConsumerGroup(String consumerGroup,
            Optional<String> namespace) throws Exception {
        log.info("Getting consumer group: {} for Aliyun instance: {}",
            consumerGroup, config.getInstanceId());

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            params.put("GroupId", consumerGroup);
            Map<String, Object> result = callApi("OnsGroupSubDetail", params);

            @SuppressWarnings("unchecked")
            Map<String, Object> groupData = (Map<String, Object>) result.get("Data");
            if (groupData == null) {
                return Optional.empty();
            }

            ConsumerGroupInfo group = new ConsumerGroupInfo();
            group.setConsumerGroupName(consumerGroup);
            group.setClusterName(config.getInstanceId());

            // Parse subscription details
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> subscriptionDatas =
                (List<Map<String, Object>>) groupData.get("SubscriptionDataList");
            if (subscriptionDatas != null) {
                group.setSubscribedTopics(new java.util.HashSet<>());
                for (Map<String, Object> sub : subscriptionDatas) {
                    String topicName = CloudApiHttpClient.getString(sub, "Topic");
                    if (topicName != null) {
                        group.getSubscribedTopics().add(topicName);
                    }
                }
            }

            return Optional.of(group);
        } catch (CloudApiException e) {
            log.warn("Failed to get consumer group {} for Aliyun instance {}: {}",
                consumerGroup, config.getInstanceId(), e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    protected void doCreateConsumerGroup(ConsumerGroupInfo consumerGroup) throws Exception {
        log.info("Creating consumer group: {} for Aliyun instance: {}",
            consumerGroup.getConsumerGroupName(), config.getInstanceId());

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            params.put("GroupId", consumerGroup.getConsumerGroupName());

            // Set group type: "tcp" for V4, "http" for HTTP
            params.put("GroupType", "tcp");

            if (consumerGroup.getAttributes() != null) {
                String remark = consumerGroup.getAttributes().get("remark");
                if (remark != null) {
                    params.put("Remark", remark);
                }
            }

            callApi("OnsGroupCreate", params);
            log.info("Consumer group created: {} for Aliyun instance: {}",
                consumerGroup.getConsumerGroupName(), config.getInstanceId());
        } catch (CloudApiException e) {
            log.error("Failed to create consumer group {} for Aliyun instance {}: {}",
                consumerGroup.getConsumerGroupName(), config.getInstanceId(), e.getMessage());
            throw e;
        }
    }

    @Override
    protected void doUpdateConsumerGroup(ConsumerGroupInfo consumerGroup) throws Exception {
        log.info("Updating consumer group: {} for Aliyun instance: {}",
            consumerGroup.getConsumerGroupName(), config.getInstanceId());
        // Aliyun consumer group properties are mostly immutable after creation
        throw new UnsupportedOperationException(
            "Aliyun consumer group update is not supported via API. "
            + "Group properties are immutable after creation.");
    }

    @Override
    protected void doDeleteConsumerGroup(String consumerGroup, Optional<String> namespace) throws Exception {
        log.info("Deleting consumer group: {} for Aliyun instance: {}",
            consumerGroup, config.getInstanceId());

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            params.put("GroupId", consumerGroup);
            callApi("OnsGroupDelete", params);
            log.info("Consumer group deleted: {} for Aliyun instance: {}",
                consumerGroup, config.getInstanceId());
        } catch (CloudApiException e) {
            log.error("Failed to delete consumer group {} for Aliyun instance {}: {}",
                consumerGroup, config.getInstanceId(), e.getMessage());
            throw e;
        }
    }

    @Override
    protected List<SubscriptionInfo> doListSubscriptions(String groupName) throws Exception {
        log.info("Listing subscriptions for group: {} instance: {}", groupName, config.getInstanceId());

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            params.put("GroupId", groupName);
            Map<String, Object> result = callApi("OnsGroupSubDetail", params);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> subList =
                (List<Map<String, Object>>) result.get("Data");
            if (subList == null) {
                // Try alternative response structure
                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>) result.get("Data");
                if (data != null) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> altSubList =
                        (List<Map<String, Object>>) data.get("SubscriptionDataList");
                    subList = altSubList;
                }
            }
            if (subList == null || subList.isEmpty()) {
                return Collections.emptyList();
            }

            List<SubscriptionInfo> subscriptions = new ArrayList<>();
            for (Map<String, Object> subData : subList) {
                SubscriptionInfo sub = new SubscriptionInfo();
                sub.setTopic(CloudApiHttpClient.getString(subData, "Topic"));
                sub.setConsumerGroup(groupName);
                sub.setSubExpression(CloudApiHttpClient.getString(subData, "SubString"));

                Object subType = subData.get("MessageType");
                if (subType != null) {
                    sub.setSubscriptionType("TAG");
                }

                subscriptions.add(sub);
            }

            return subscriptions;
        } catch (CloudApiException e) {
            log.warn("Failed to list subscriptions for group {} in Aliyun instance {}: {}",
                groupName, config.getInstanceId(), e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    protected void doResetConsumerGroupOffset(String groupName, String topic, long timestamp)
            throws Exception {
        log.info("Resetting offset for group: {} topic: {} instance: {}",
            groupName, topic, config.getInstanceId());

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            params.put("GroupId", groupName);
            params.put("Topic", topic);
            // Type: 1 = by timestamp, 2 = to max offset
            params.put("Type", "1");
            params.put("ResetTimestamp", String.valueOf(timestamp));
            callApi("OnsConsumerResetOffset", params);
            log.info("Offset reset successfully for group: {} topic: {} in Aliyun instance: {}",
                groupName, topic, config.getInstanceId());
        } catch (CloudApiException e) {
            log.error("Failed to reset offset for group {} topic {} in Aliyun instance {}: {}",
                groupName, topic, config.getInstanceId(), e.getMessage());
            throw e;
        }
    }

    // ==================== Message Operations ====================

    @Override
    protected List<MessageInfo> doQueryMessageByTopic(String topic, long beginTime, long endTime,
            int maxNum) throws Exception {
        log.info("Querying messages for topic: {} instance: {} timeRange=[{}, {}]",
            topic, config.getInstanceId(), beginTime, endTime);

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            params.put("Topic", topic);
            params.put("BeginTime", String.valueOf(beginTime / 1000)); // Aliyun expects seconds
            params.put("EndTime", String.valueOf(endTime / 1000));
            params.put("CurrentPage", "1");
            if (maxNum > 0) {
                params.put("PageSize", String.valueOf(Math.min(maxNum, 20))); // Max 20 per page
            }

            Map<String, Object> result = callApi("OnsMessagePageQueryByTopic", params);

            @SuppressWarnings("unchecked")
            Map<String, Object> pageData = (Map<String, Object>) result.get("Data");
            if (pageData == null) {
                return Collections.emptyList();
            }

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> msgList =
                (List<Map<String, Object>>) pageData.get("MessageList");
            if (msgList == null || msgList.isEmpty()) {
                return Collections.emptyList();
            }

            List<MessageInfo> messages = new ArrayList<>();
            for (Map<String, Object> msgData : msgList) {
                messages.add(mapToMessageInfo(msgData));
            }

            return messages;
        } catch (CloudApiException e) {
            log.error("Failed to query messages for topic {} in Aliyun instance {}: {}",
                topic, config.getInstanceId(), e.getMessage());
            throw e;
        }
    }

    @Override
    protected List<MessageInfo> doQueryMessageByTopicAndKey(String topic, String key,
            long beginTime, long endTime) throws Exception {
        log.info("Querying messages by key: {} for topic: {} instance: {}",
            key, topic, config.getInstanceId());

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            params.put("Topic", topic);
            params.put("MsgKey", key);
            params.put("BeginTime", String.valueOf(beginTime / 1000));
            params.put("EndTime", String.valueOf(endTime / 1000));

            Map<String, Object> result = callApi("OnsMessageGetByKey", params);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> msgList =
                (List<Map<String, Object>>) result.get("Data");
            if (msgList == null || msgList.isEmpty()) {
                return Collections.emptyList();
            }

            List<MessageInfo> messages = new ArrayList<>();
            for (Map<String, Object> msgData : msgList) {
                messages.add(mapToMessageInfo(msgData));
            }

            return messages;
        } catch (CloudApiException e) {
            log.error("Failed to query messages by key {} for topic {} in Aliyun instance {}: {}",
                key, topic, config.getInstanceId(), e.getMessage());
            throw e;
        }
    }

    @Override
    protected List<MessageInfo> doQueryMessageByGroup(String consumerGroup, String topic,
            long beginTime, long endTime) throws Exception {
        log.info("Querying messages by group: {} topic: {} instance: {}",
            consumerGroup, topic, config.getInstanceId());
        // Aliyun ONS does not have a direct group-based message query API.
        // This would require combining consumer status with message query.
        log.warn("Aliyun ONS does not support direct group-based message query. "
            + "Use topic-based query instead.");
        return Collections.emptyList();
    }

    @Override
    protected Optional<MessageInfo> doGetMessageById(String msgId) throws Exception {
        log.info("Getting message by ID: {} instance: {}", msgId, config.getInstanceId());

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("InstanceId", config.getInstanceId());
            params.put("MsgId", msgId);

            Map<String, Object> result = callApi("OnsMessageGetByMsgId", params);

            @SuppressWarnings("unchecked")
            Map<String, Object> msgData = (Map<String, Object>) result.get("Data");
            if (msgData == null) {
                return Optional.empty();
            }

            return Optional.of(mapToMessageInfo(msgData));
        } catch (CloudApiException e) {
            log.warn("Failed to get message by ID {} in Aliyun instance {}: {}",
                msgId, config.getInstanceId(), e.getMessage());
            return Optional.empty();
        }
    }

    // ==================== Client Operations ====================

    @Override
    protected List<ClientInstance> doListClientInstances(Optional<String> topic,
            Optional<String> group) throws Exception {
        log.info("Listing client instances for Aliyun instance: {}", config.getInstanceId());

        // Aliyun ONS provides consumer status which includes client connection info
        if (group.isPresent()) {
            try {
                Map<String, String> params = new LinkedHashMap<>();
                params.put("InstanceId", config.getInstanceId());
                params.put("GroupId", group.get());
                Map<String, Object> result = callApi("OnsConsumerStatus", params);

                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>) result.get("Data");
                if (data == null) {
                    return Collections.emptyList();
                }

                @SuppressWarnings("unchecked")
                List<Map<String, Object>> connections =
                    (List<Map<String, Object>>) data.get("ConnectionList");
                if (connections == null || connections.isEmpty()) {
                    return Collections.emptyList();
                }

                List<ClientInstance> clients = new ArrayList<>();
                for (Map<String, Object> conn : connections) {
                    ClientInstance client = new ClientInstance();
                    client.setClientId(CloudApiHttpClient.getString(conn, "ClientId"));
                    client.setClientAddress(CloudApiHttpClient.getString(conn, "ClientAddr"));
                    client.setConsumerGroup(group.get());
                    client.setClientType(ClientInstance.ClientType.CONSUMER);
                    client.setActive(true);

                    String language = CloudApiHttpClient.getString(conn, "Language");
                    client.setLanguage(language);

                    String version = CloudApiHttpClient.getString(conn, "Version");
                    client.setSdkVersion(version);

                    clients.add(client);
                }

                return clients;
            } catch (CloudApiException e) {
                log.warn("Failed to list client instances for group {} in Aliyun instance {}: {}",
                    group.get(), config.getInstanceId(), e.getMessage());
                return Collections.emptyList();
            }
        }

        // Without a specific group, Aliyun does not expose a general client listing API
        log.warn("Aliyun ONS does not expose a general client listing API. "
            + "Provide a consumer group to list its connected clients.");
        return Collections.emptyList();
    }

    @Override
    protected Optional<ClientInstance> doGetClientInstance(String clientId) throws Exception {
        log.info("Getting client instance: {} for Aliyun instance: {}", clientId, config.getInstanceId());
        // Aliyun does not provide a direct API to get a single client by ID
        return Optional.empty();
    }

    // ==================== Aliyun API Signing & Calling ====================

    /**
     * Call an Aliyun RocketMQ API action.
     *
     * @param action the API action name
     * @param extraParams additional parameters
     * @return parsed JSON response
     */
    private Map<String, Object> callApi(String action, Map<String, String> extraParams)
            throws CloudApiException {
        LinkedHashMap<String, String> params = new LinkedHashMap<>();
        params.putAll(buildCommonParams(action));
        if (extraParams != null) {
            params.putAll(extraParams);
        }

        // Sign the request
        String signature = signRequest(params);
        params.put("Signature", signature);

        // Build URL and execute
        String url = CloudApiHttpClient.buildUrl("https://" + resolvedEndpoint, params);
        String responseBody = httpClient.get(url, null);

        // Parse and check for API errors
        Map<String, Object> result = CloudApiHttpClient.parseJson(responseBody);
        checkApiError(result);

        return result;
    }

    /**
     * Build common parameters for every Aliyun API call.
     */
    private Map<String, String> buildCommonParams(String action) {
        LinkedHashMap<String, String> params = new LinkedHashMap<>();
        params.put("Action", action);
        params.put("Version", API_VERSION);
        params.put("Format", "JSON");
        params.put("AccessKeyId", config.getAccessKey());
        params.put("SignatureMethod", SIGNATURE_METHOD);
        params.put("SignatureVersion", SIGNATURE_VERSION);
        params.put("SignatureNonce", UUID.randomUUID().toString());
        params.put("Timestamp", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .withZone(ZoneOffset.UTC)
            .format(Instant.now()));
        params.put("RegionId", config.getRegionId() != null ? config.getRegionId() : "cn-hangzhou");
        return params;
    }

    /**
     * Sign the request using Aliyun Signature V1.0 (HMAC-SHA1).
     */
    private String signRequest(LinkedHashMap<String, String> params) throws CloudApiException {
        // Sort parameters by key
        LinkedHashMap<String, String> sorted = new LinkedHashMap<>();
        params.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(e -> sorted.put(e.getKey(), e.getValue()));

        // Build canonicalized query string
        String canonicalizedQueryString = CloudApiHttpClient.buildQueryString(sorted);

        // Build string to sign
        String stringToSign = "GET&" + CloudApiHttpClient.percentEncode("/") + "&"
            + CloudApiHttpClient.percentEncode(canonicalizedQueryString);

        // Sign with HMAC-SHA1
        String signKey = config.getSecretKey() + "&";
        return CloudApiHttpClient.hmacSha1(stringToSign, signKey);
    }

    /**
     * Check if the API response contains an error.
     */
    private void checkApiError(Map<String, Object> result) throws CloudApiException {
        String code = CloudApiHttpClient.getString(result, "Code");
        if (code != null && !code.isEmpty() && !"200".equals(code) && !"OK".equals(code)) {
            String message = CloudApiHttpClient.getString(result, "Message");
            String requestId = CloudApiHttpClient.getString(result, "RequestId");
            throw new CloudApiException(
                String.format("Aliyun API error: code=%s, message=%s, requestId=%s",
                    code, message, requestId));
        }
    }

    // ==================== Mapping Helpers ====================

    private TopicInfo mapToTopicInfo(Map<String, Object> data) {
        TopicInfo topic = new TopicInfo();
        topic.setTopicName(CloudApiHttpClient.getString(data, "Topic"));
        topic.setClusterName(config.getInstanceId());

        // Map Aliyun message type to TopicType
        int msgType = CloudApiHttpClient.getInt(data, "MessageType", 0);
        topic.setTopicType(mapAliyunMessageTypeToTopicType(msgType));

        Long createTime = (Long) data.get("CreateTime");
        if (createTime != null) {
            topic.setCreateTime(new Date(createTime));
        }

        Long updateTime = (Long) data.get("UpdateTime");
        if (updateTime != null) {
            topic.setUpdateTime(new Date(updateTime));
        }

        // Set queue numbers from response
        topic.setReadQueueNums(CloudApiHttpClient.getInt(data, "ReadQueueNums", 8));
        topic.setWriteQueueNums(CloudApiHttpClient.getInt(data, "WriteQueueNums", 8));

        // Store remark in attributes
        String remark = CloudApiHttpClient.getString(data, "Remark");
        if (remark != null) {
            Map<String, String> attrs = new HashMap<>();
            attrs.put("remark", remark);
            topic.setAttributes(attrs);
        }

        return topic;
    }

    private ConsumerGroupInfo mapToConsumerGroupInfo(Map<String, Object> data) {
        ConsumerGroupInfo group = new ConsumerGroupInfo();
        group.setConsumerGroupName(CloudApiHttpClient.getString(data, "GroupId"));
        group.setClusterName(config.getInstanceId());

        Long createTime = (Long) data.get("CreateTime");
        if (createTime != null) {
            group.setCreateTime(new Date(createTime));
        }

        Long updateTime = (Long) data.get("UpdateTime");
        if (updateTime != null) {
            group.setUpdateTime(new Date(updateTime));
        }

        // Aliyun group type mapping
        String groupType = CloudApiHttpClient.getString(data, "GroupType");
        if ("http".equalsIgnoreCase(groupType)) {
            group.setConsumeMode("POP");
        } else {
            group.setConsumeMode("PUSH");
        }

        String remark = CloudApiHttpClient.getString(data, "Remark");
        if (remark != null) {
            Map<String, String> attrs = new HashMap<>();
            attrs.put("remark", remark);
            group.setAttributes(attrs);
        }

        return group;
    }

    private MessageInfo mapToMessageInfo(Map<String, Object> data) {
        MessageInfo msg = new MessageInfo();
        msg.setMsgId(CloudApiHttpClient.getString(data, "MsgId"));
        msg.setTopic(CloudApiHttpClient.getString(data, "Topic"));
        msg.setTags(CloudApiHttpClient.getString(data, "Tag"));
        msg.setKeys(CloudApiHttpClient.getString(data, "Key"));
        msg.setBody(CloudApiHttpClient.getString(data, "Body"));

        Long bornAt = (Long) data.get("BornTimestamp");
        if (bornAt != null) {
            msg.setBornTimestamp(bornAt);
        }

        Long storeAt = (Long) data.get("StoreTimestamp");
        if (storeAt != null) {
            msg.setStoreTimestamp(storeAt);
        }

        msg.setQueueId(CloudApiHttpClient.getInt(data, "QueueId", 0));
        msg.setQueueOffset(CloudApiHttpClient.getLong(data, "QueueOffset", 0));
        msg.setBrokerName(CloudApiHttpClient.getString(data, "BrokerName"));
        msg.setReconsumeTimes(CloudApiHttpClient.getInt(data, "ReconsumeTimes", 0));

        return msg;
    }

    /**
     * Map TopicType to Aliyun message type integer.
     * 0=Normal, 1=FIFO, 2=Delay, 3=Transaction
     */
    private int mapTopicTypeToAliyunMessageType(TopicType type) {
        if (type == null) {
            return 0;
        }
        switch (type) {
            case FIFO:
                return 1;
            case DELAY:
                return 2;
            case TRANSACTION:
                return 3;
            case NORMAL:
            default:
                return 0;
        }
    }

    /**
     * Map Aliyun message type integer to TopicType.
     */
    private TopicType mapAliyunMessageTypeToTopicType(int msgType) {
        switch (msgType) {
            case 1:
                return TopicType.FIFO;
            case 2:
                return TopicType.DELAY;
            case 3:
                return TopicType.TRANSACTION;
            case 0:
            default:
                return TopicType.NORMAL;
        }
    }

    /**
     * Map Aliyun instance status code to a status string.
     * 1=deploying, 2=running, 5=frozen
     */
    private String mapInstanceStatus(int statusCode) {
        switch (statusCode) {
            case 1:
                return "DEPLOYING";
            case 2:
                return "ENABLED";
            case 5:
                return "FROZEN";
            default:
                return "UNKNOWN";
        }
    }
}
