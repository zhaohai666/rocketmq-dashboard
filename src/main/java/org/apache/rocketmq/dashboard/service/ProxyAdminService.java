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
package org.apache.rocketmq.dashboard.service;

import java.util.List;
import java.util.Map;

/**
 * Service interface for RIP-2 Proxy Admin operations (M2/M3/M4).
 *
 * <p>Provides Dashboard-layer access to:
 * <ul>
 *   <li>M2: Runtime configuration query/hot-update, client disconnection</li>
 *   <li>M3: POP receipt handle diagnostics</li>
 *   <li>M4: Batch consumption diagnostics</li>
 *   <li>Route change event subscription</li>
 * </ul>
 *
 * <p>All operations aggregate results from multiple Proxy instances when
 * deployed in a multi-Proxy cluster topology.</p>
 */
public interface ProxyAdminService {

    // ==================== M2: Config & Connection Management ====================

    /**
     * Get runtime configuration from all proxy instances.
     *
     * @return map of proxyAddress → config snapshot (as nested map of field→value)
     */
    Map<String, Map<String, Object>> getProxyConfigs();

    /**
     * Update runtime configuration on all proxy instances.
     *
     * @param configUpdates map of field name → new value
     * @return map of proxyAddress → list of changed field names
     */
    Map<String, List<String>> updateProxyConfig(Map<String, Object> configUpdates);

    /**
     * Force disconnect a client from the proxy cluster.
     *
     * @param clientId the unique client identifier
     * @param reason   human-readable reason for audit
     * @return true if disconnected from at least one proxy
     */
    boolean disconnectClient(String clientId, String reason);

    // ==================== M3: POP Diagnostics ====================

    /**
     * Query POP receipt handle diagnostics for a consumer group.
     *
     * @param group    consumer group name
     * @param topic    optional topic filter
     * @param pageNum  page number (1-based)
     * @param pageSize page size
     * @return aggregated POP diagnostics result
     */
    PopDiagnosticsResult describePopReceiptHandles(String group, String topic, int pageNum, int pageSize);

    // ==================== M4: Batch Consumption Diagnostics ====================

    /**
     * Query batch consumption diagnostics for a consumer group.
     *
     * @param group    consumer group name
     * @param topic    optional topic filter
     * @param clientId optional client ID filter
     * @param pageNum  page number (1-based)
     * @param pageSize page size
     * @return aggregated batch consumption diagnostics result
     */
    BatchConsumeDiagnosticsResult describeBatchConsumeDiagnostics(
        String group, String topic, String clientId, int pageNum, int pageSize);

    // ==================== Status ====================

    /**
     * Get proxy admin connection status.
     *
     * @return status info including available/total proxy count
     */
    ProxyAdminStatus getStatus();

    // ==================== Result Types ====================

    class PopDiagnosticsResult {
        private int totalHandles;
        private int totalMessages;
        private long totalRenewTimes;
        private long totalRenewRetryTimes;
        private int expiredHandles;
        private List<Map<String, Object>> handles;
        private int total;

        public int getTotalHandles() { return totalHandles; }
        public void setTotalHandles(int totalHandles) { this.totalHandles = totalHandles; }
        public int getTotalMessages() { return totalMessages; }
        public void setTotalMessages(int totalMessages) { this.totalMessages = totalMessages; }
        public long getTotalRenewTimes() { return totalRenewTimes; }
        public void setTotalRenewTimes(long totalRenewTimes) { this.totalRenewTimes = totalRenewTimes; }
        public long getTotalRenewRetryTimes() { return totalRenewRetryTimes; }
        public void setTotalRenewRetryTimes(long totalRenewRetryTimes) { this.totalRenewRetryTimes = totalRenewRetryTimes; }
        public int getExpiredHandles() { return expiredHandles; }
        public void setExpiredHandles(int expiredHandles) { this.expiredHandles = expiredHandles; }
        public List<Map<String, Object>> getHandles() { return handles; }
        public void setHandles(List<Map<String, Object>> handles) { this.handles = handles; }
        public int getTotal() { return total; }
        public void setTotal(int total) { this.total = total; }
    }

    class BatchConsumeDiagnosticsResult {
        private int totalClients;
        private int totalUnackedMessages;
        private int totalUnackedHandles;
        private int totalExpiredHandles;
        private long totalRenewTimes;
        private long totalRenewRetryTimes;
        private List<Map<String, Object>> diagnostics;
        private int total;

        public int getTotalClients() { return totalClients; }
        public void setTotalClients(int totalClients) { this.totalClients = totalClients; }
        public int getTotalUnackedMessages() { return totalUnackedMessages; }
        public void setTotalUnackedMessages(int totalUnackedMessages) { this.totalUnackedMessages = totalUnackedMessages; }
        public int getTotalUnackedHandles() { return totalUnackedHandles; }
        public void setTotalUnackedHandles(int totalUnackedHandles) { this.totalUnackedHandles = totalUnackedHandles; }
        public int getTotalExpiredHandles() { return totalExpiredHandles; }
        public void setTotalExpiredHandles(int totalExpiredHandles) { this.totalExpiredHandles = totalExpiredHandles; }
        public long getTotalRenewTimes() { return totalRenewTimes; }
        public void setTotalRenewTimes(long totalRenewTimes) { this.totalRenewTimes = totalRenewTimes; }
        public long getTotalRenewRetryTimes() { return totalRenewRetryTimes; }
        public void setTotalRenewRetryTimes(long totalRenewRetryTimes) { this.totalRenewRetryTimes = totalRenewRetryTimes; }
        public List<Map<String, Object>> getDiagnostics() { return diagnostics; }
        public void setDiagnostics(List<Map<String, Object>> diagnostics) { this.diagnostics = diagnostics; }
        public int getTotal() { return total; }
        public void setTotal(int total) { this.total = total; }
    }

    class ProxyAdminStatus {
        private int availableProxies;
        private int totalProxies;
        private String[] proxyAddresses;
        private boolean connected;

        public int getAvailableProxies() { return availableProxies; }
        public void setAvailableProxies(int availableProxies) { this.availableProxies = availableProxies; }
        public int getTotalProxies() { return totalProxies; }
        public void setTotalProxies(int totalProxies) { this.totalProxies = totalProxies; }
        public String[] getProxyAddresses() { return proxyAddresses; }
        public void setProxyAddresses(String[] proxyAddresses) { this.proxyAddresses = proxyAddresses; }
        public boolean isConnected() { return connected; }
        public void setConnected(boolean connected) { this.connected = connected; }
    }
}
