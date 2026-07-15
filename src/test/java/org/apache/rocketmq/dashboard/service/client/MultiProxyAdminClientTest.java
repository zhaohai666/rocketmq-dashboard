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

import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MultiProxyAdminClientTest {

    private MultiProxyAdminClient client;

    private static final String[] TEST_ADDRESSES = {"host1:8080", "host2:8080", "host3:8080"};

    @Before
    public void setUp() {
        client = new MultiProxyAdminClient(TEST_ADDRESSES);
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.shutdown();
        }
    }

    // ==================== Construction ====================

    @Test
    public void testConstructionWithMultipleAddresses() {
        assertEquals(3, client.getTotalCount());
        assertEquals(3, client.getClients().size());
        assertNotNull(client.getProxyAddresses());
        assertEquals(3, client.getProxyAddresses().length);
    }

    @Test
    public void testConstructionWithSingleAddress() {
        MultiProxyAdminClient singleClient = new MultiProxyAdminClient(new String[]{"localhost:8080"});
        assertEquals(1, singleClient.getTotalCount());
        assertEquals(1, singleClient.getClients().size());
        singleClient.shutdown();
    }

    @Test
    public void testConstructionWithEmptyAddresses() {
        MultiProxyAdminClient emptyClient = new MultiProxyAdminClient(new String[]{});
        assertEquals(0, emptyClient.getTotalCount());
        assertEquals(0, emptyClient.getClients().size());
        assertFalse(emptyClient.isAvailable());
        emptyClient.shutdown();
    }

    @Test
    public void testConstructionWithNullAddresses() {
        MultiProxyAdminClient nullClient = new MultiProxyAdminClient(null);
        assertEquals(0, nullClient.getTotalCount());
        assertFalse(nullClient.isAvailable());
        nullClient.shutdown();
    }

    @Test
    public void testConstructionWithCustomPort() {
        MultiProxyAdminClient customPortClient = new MultiProxyAdminClient(TEST_ADDRESSES, 9999);
        assertEquals(3, customPortClient.getTotalCount());
        customPortClient.shutdown();
    }

    // ==================== Availability ====================

    @Test
    public void testIsAvailableReturnsFalseBeforeConnection() {
        // Before any connection attempt, clients are not initialized
        assertFalse("Should not be available before connection", client.isAvailable());
    }

    @Test
    public void testGetAvailableCountReturnsZeroBeforeConnection() {
        assertEquals(0, client.getAvailableCount());
    }

    // ==================== M1: Client Queries (graceful degradation) ====================

    @Test
    public void testListClientsReturnsEmptyWhenNoProxyAvailable() {
        List<?> result = client.listClients("group", "topic", null, 1, 100);
        assertNotNull(result);
        assertTrue("Should return empty when no proxy is available", result.isEmpty());
    }

    @Test
    public void testListClientsReturnsEmptyWithNullFilters() {
        List<?> result = client.listClients(null, null, null, 1, 100);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testDescribeClientReturnsNullWhenNoProxyAvailable() {
        Object result = client.describeClient("testClient");
        assertNull("Should return null when no proxy available", result);
    }

    @Test
    public void testDescribeClientWithNullClientId() {
        Object result = client.describeClient(null);
        assertNull(result);
    }

    @Test
    public void testListClientsByGroupReturnsEmptyWhenNoProxyAvailable() {
        List<?> result = client.listClientsByGroup("group1", 1, 10);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListClientsByGroupWithNullGroup() {
        List<?> result = client.listClientsByGroup(null, 1, 10);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListClientsByTopicReturnsEmptyWhenNoProxyAvailable() {
        List<?> result = client.listClientsByTopic("topic1", 1, 10);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testListClientsByTopicWithNullTopic() {
        List<?> result = client.listClientsByTopic(null, 1, 10);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // ==================== M2: Config & Connection Management ====================

    @Test
    public void testGetConfigFromAllReturnsEmptyWhenNoProxyAvailable() {
        Map<?, ?> result = client.getConfigFromAll();
        assertNotNull(result);
        assertTrue("Should return empty map when no proxy available", result.isEmpty());
    }

    @Test
    public void testGetConfigReturnsNullWhenNoProxyAvailable() {
        Object result = client.getConfig();
        assertNull("Should return null when no proxy available", result);
    }

    @Test
    public void testUpdateConfigAllReturnsMapWithNullValues() {
        // Proto builder returns non-null empty config
        apache.rocketmq.proxy.admin.v1.ProxyRuntimeConfig config =
            apache.rocketmq.proxy.admin.v1.ProxyRuntimeConfig.newBuilder().build();

        Map<?, ?> result = client.updateConfigAll(config);
        assertNotNull(result);
        // Each proxy will have null response since they can't connect
        assertEquals(3, result.size());
    }

    @Test
    public void testDisconnectClientReturnsFalseWhenNoProxyAvailable() {
        boolean result = client.disconnectClient("clientId", "test reason");
        assertFalse("Should return false when no proxy can disconnect", result);
    }

    @Test
    public void testDisconnectClientWithNullClientId() {
        boolean result = client.disconnectClient(null, "reason");
        assertFalse(result);
    }

    @Test
    public void testDisconnectClientWithEmptyClientId() {
        boolean result = client.disconnectClient("", "reason");
        assertFalse(result);
    }

    // ==================== M3: POP Diagnostics ====================

    @Test
    public void testDescribePopReceiptHandlesFromAllReturnsEmptyWhenNoProxy() {
        List<?> result = client.describePopReceiptHandlesFromAll("group1", "topic1", 1, 20);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testDescribePopReceiptHandlesWithNullGroup() {
        List<?> result = client.describePopReceiptHandlesFromAll(null, null, 1, 20);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // ==================== M4: Batch Consumption Diagnostics ====================

    @Test
    public void testDescribeBatchConsumeDiagnosticsFromAllReturnsEmptyWhenNoProxy() {
        List<?> result = client.describeBatchConsumeDiagnosticsFromAll(
            "group1", "topic1", null, 1, 20);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testDescribeBatchConsumeDiagnosticsWithNullGroup() {
        List<?> result = client.describeBatchConsumeDiagnosticsFromAll(
            null, null, null, 1, 20);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    // ==================== Lifecycle ====================

    @Test
    public void testShutdownCompletesSuccessfully() {
        MultiProxyAdminClient shutdownClient = new MultiProxyAdminClient(TEST_ADDRESSES);
        shutdownClient.shutdown();
        assertFalse("Should not be available after shutdown", shutdownClient.isAvailable());
        assertEquals(0, shutdownClient.getAvailableCount());
    }

    @Test
    public void testDoubleShutdownDoesNotThrow() {
        MultiProxyAdminClient shutdownClient = new MultiProxyAdminClient(TEST_ADDRESSES);
        shutdownClient.shutdown();
        shutdownClient.shutdown(); // Should not throw
    }

    // ==================== getClients immutability ====================

    @Test(expected = UnsupportedOperationException.class)
    public void testGetClientsReturnsUnmodifiableList() {
        client.getClients().add(new ProxyAdminGrpcClient("newhost:8080"));
    }
}
