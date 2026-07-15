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

import com.alibaba.fastjson.JSON;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.dashboard.service.ProxyAdminService;
import org.apache.rocketmq.dashboard.service.ProxyAdminService.BatchConsumeDiagnosticsResult;
import org.apache.rocketmq.dashboard.service.ProxyAdminService.PopDiagnosticsResult;
import org.apache.rocketmq.dashboard.service.ProxyAdminService.ProxyAdminStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class ProxyAdminControllerTest extends BaseControllerTest {

    @InjectMocks
    private ProxyAdminController proxyAdminController;

    @Mock
    private ProxyAdminService proxyAdminService;

    @Before
    public void init() {
        super.mockRmqConfigure();
    }

    @Override
    protected Object getTestController() {
        return proxyAdminController;
    }

    // ==================== GET /status ====================

    @Test
    public void testGetStatus() throws Exception {
        ProxyAdminStatus status = new ProxyAdminStatus();
        status.setConnected(true);
        status.setAvailableProxies(2);
        status.setTotalProxies(3);
        status.setProxyAddresses(new String[]{"host1:8080", "host2:8080", "host3:8080"});

        when(proxyAdminService.getStatus()).thenReturn(status);

        requestBuilder = MockMvcRequestBuilders.get("/api/proxy/admin/status");
        perform = mockMvc.perform(requestBuilder);
        performOkExpect(perform)
            .andExpect(jsonPath("$.data.connected").value(true))
            .andExpect(jsonPath("$.data.availableProxies").value(2))
            .andExpect(jsonPath("$.data.totalProxies").value(3));
    }

    @Test
    public void testGetStatusException() throws Exception {
        when(proxyAdminService.getStatus()).thenThrow(new RuntimeException("connection error"));

        requestBuilder = MockMvcRequestBuilders.get("/api/proxy/admin/status");
        perform = mockMvc.perform(requestBuilder);
        perform.andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value(1))
            .andExpect(jsonPath("$.errMsg").exists());
    }

    // ==================== GET /config ====================

    @Test
    public void testGetConfig() throws Exception {
        Map<String, Map<String, Object>> configs = new LinkedHashMap<>();
        Map<String, Object> proxyConfig = new LinkedHashMap<>();
        proxyConfig.put("proxyMode", "CLUSTER");
        proxyConfig.put("proxyName", "proxy-0");
        proxyConfig.put("maxMessageSize", 4194304);
        configs.put("host1:8082", proxyConfig);

        when(proxyAdminService.getProxyConfigs()).thenReturn(configs);

        requestBuilder = MockMvcRequestBuilders.get("/api/proxy/admin/config");
        perform = mockMvc.perform(requestBuilder);
        performOkExpect(perform)
            .andExpect(jsonPath("$.data.['host1:8082'].proxyMode").value("CLUSTER"))
            .andExpect(jsonPath("$.data.['host1:8082'].proxyName").value("proxy-0"))
            .andExpect(jsonPath("$.data.['host1:8082'].maxMessageSize").value(4194304));
    }

    @Test
    public void testGetConfigEmpty() throws Exception {
        when(proxyAdminService.getProxyConfigs()).thenReturn(Collections.emptyMap());

        requestBuilder = MockMvcRequestBuilders.get("/api/proxy/admin/config");
        perform = mockMvc.perform(requestBuilder);
        performOkExpect(perform);
    }

    // ==================== POST /config ====================

    @Test
    public void testUpdateConfig() throws Exception {
        Map<String, List<String>> result = new LinkedHashMap<>();
        result.put("host1:8082", Arrays.asList("maxMessageSize", "traceOn"));

        when(proxyAdminService.updateProxyConfig(any())).thenReturn(result);

        Map<String, Object> updates = new HashMap<>();
        updates.put("maxMessageSize", 8388608);
        updates.put("traceOn", true);

        requestBuilder = MockMvcRequestBuilders.post("/api/proxy/admin/config")
            .contentType(MediaType.APPLICATION_JSON)
            .content(JSON.toJSONString(updates));
        perform = mockMvc.perform(requestBuilder);
        performOkExpect(perform)
            .andExpect(jsonPath("$.data.['host1:8082']").isArray());
    }

    @Test
    public void testUpdateConfigEmptyBody() throws Exception {
        requestBuilder = MockMvcRequestBuilders.post("/api/proxy/admin/config")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{}");
        perform = mockMvc.perform(requestBuilder);
        perform.andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value(1))
            .andExpect(jsonPath("$.errMsg").value("Config updates cannot be empty"));
    }

    // ==================== POST /disconnect/{clientId} ====================

    @Test
    public void testDisconnectClientSuccess() throws Exception {
        when(proxyAdminService.disconnectClient(eq("client-001"), anyString())).thenReturn(true);

        requestBuilder = MockMvcRequestBuilders.post("/api/proxy/admin/disconnect/client-001");
        perform = mockMvc.perform(requestBuilder);
        performOkExpect(perform)
            .andExpect(jsonPath("$.data.disconnected").value(true))
            .andExpect(jsonPath("$.data.clientId").value("client-001"));
    }

    @Test
    public void testDisconnectClientNotFound() throws Exception {
        when(proxyAdminService.disconnectClient(anyString(), anyString())).thenReturn(false);

        requestBuilder = MockMvcRequestBuilders.post("/api/proxy/admin/disconnect/unknown-client");
        perform = mockMvc.perform(requestBuilder);
        perform.andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value(1));
    }

    @Test
    public void testDisconnectClientWithReason() throws Exception {
        when(proxyAdminService.disconnectClient(eq("client-001"), eq("misbehaving"))).thenReturn(true);

        requestBuilder = MockMvcRequestBuilders.post("/api/proxy/admin/disconnect/client-001")
            .param("reason", "misbehaving");
        perform = mockMvc.perform(requestBuilder);
        performOkExpect(perform);
    }

    // ==================== GET /pop-diagnostics ====================

    @Test
    public void testDescribePopReceiptHandles() throws Exception {
        PopDiagnosticsResult result = new PopDiagnosticsResult();
        result.setTotalHandles(10);
        result.setTotalMessages(20);
        result.setTotalRenewTimes(5);
        result.setTotalRenewRetryTimes(1);
        result.setExpiredHandles(2);
        result.setTotal(3);

        List<Map<String, Object>> handles = new ArrayList<>();
        Map<String, Object> handle = new HashMap<>();
        handle.put("group", "testGroup");
        handle.put("topic", "testTopic");
        handle.put("messageId", "msg001");
        handles.add(handle);
        result.setHandles(handles);

        when(proxyAdminService.describePopReceiptHandles(eq("testGroup"), isNull(), eq(1), eq(20)))
            .thenReturn(result);

        requestBuilder = MockMvcRequestBuilders.get("/api/proxy/admin/pop-diagnostics")
            .param("group", "testGroup");
        perform = mockMvc.perform(requestBuilder);
        performOkExpect(perform)
            .andExpect(jsonPath("$.data.totalHandles").value(10))
            .andExpect(jsonPath("$.data.totalMessages").value(20))
            .andExpect(jsonPath("$.data.total").value(3))
            .andExpect(jsonPath("$.data.handles[0].group").value("testGroup"));
    }

    @Test
    public void testDescribePopReceiptHandlesWithTopicFilter() throws Exception {
        PopDiagnosticsResult result = new PopDiagnosticsResult();
        result.setHandles(new ArrayList<>());
        result.setTotalHandles(0);

        when(proxyAdminService.describePopReceiptHandles(eq("group"), eq("topic"), eq(2), eq(10)))
            .thenReturn(result);

        requestBuilder = MockMvcRequestBuilders.get("/api/proxy/admin/pop-diagnostics")
            .param("group", "group")
            .param("topic", "topic")
            .param("pageNum", "2")
            .param("pageSize", "10");
        perform = mockMvc.perform(requestBuilder);
        performOkExpect(perform);
    }

    @Test
    public void testDescribePopReceiptHandlesEmptyGroup() throws Exception {
        requestBuilder = MockMvcRequestBuilders.get("/api/proxy/admin/pop-diagnostics")
            .param("group", "  ");
        perform = mockMvc.perform(requestBuilder);
        perform.andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value(1))
            .andExpect(jsonPath("$.errMsg").value("Consumer group name is required"));
    }

    // ==================== GET /batch-diagnostics ====================

    @Test
    public void testDescribeBatchConsumeDiagnostics() throws Exception {
        BatchConsumeDiagnosticsResult result = new BatchConsumeDiagnosticsResult();
        result.setTotalClients(5);
        result.setTotalUnackedMessages(100);
        result.setTotalUnackedHandles(50);
        result.setTotalExpiredHandles(3);
        result.setTotal(2);

        List<Map<String, Object>> diagnostics = new ArrayList<>();
        Map<String, Object> diag = new HashMap<>();
        diag.put("clientId", "client-001");
        diag.put("unackedMessageCount", 20);
        diagnostics.add(diag);
        result.setDiagnostics(diagnostics);

        when(proxyAdminService.describeBatchConsumeDiagnostics(
            eq("testGroup"), isNull(), isNull(), eq(1), eq(20)))
            .thenReturn(result);

        requestBuilder = MockMvcRequestBuilders.get("/api/proxy/admin/batch-diagnostics")
            .param("group", "testGroup");
        perform = mockMvc.perform(requestBuilder);
        performOkExpect(perform)
            .andExpect(jsonPath("$.data.totalClients").value(5))
            .andExpect(jsonPath("$.data.totalUnackedMessages").value(100))
            .andExpect(jsonPath("$.data.total").value(2))
            .andExpect(jsonPath("$.data.diagnostics[0].clientId").value("client-001"));
    }

    @Test
    public void testDescribeBatchConsumeDiagnosticsWithFilters() throws Exception {
        BatchConsumeDiagnosticsResult result = new BatchConsumeDiagnosticsResult();
        result.setDiagnostics(new ArrayList<>());
        result.setTotalClients(1);

        when(proxyAdminService.describeBatchConsumeDiagnostics(
            eq("group"), eq("topic"), eq("client-001"), eq(1), eq(50)))
            .thenReturn(result);

        requestBuilder = MockMvcRequestBuilders.get("/api/proxy/admin/batch-diagnostics")
            .param("group", "group")
            .param("topic", "topic")
            .param("clientId", "client-001")
            .param("pageSize", "50");
        perform = mockMvc.perform(requestBuilder);
        performOkExpect(perform);
    }

    @Test
    public void testDescribeBatchConsumeDiagnosticsEmptyGroup() throws Exception {
        requestBuilder = MockMvcRequestBuilders.get("/api/proxy/admin/batch-diagnostics")
            .param("group", "");
        perform = mockMvc.perform(requestBuilder);
        perform.andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value(1))
            .andExpect(jsonPath("$.errMsg").value("Consumer group name is required"));
    }
}
