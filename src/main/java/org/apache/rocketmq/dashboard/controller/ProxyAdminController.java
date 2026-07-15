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

import org.apache.rocketmq.dashboard.service.ProxyAdminService;
import org.apache.rocketmq.dashboard.service.ProxyAdminService.BatchConsumeDiagnosticsResult;
import org.apache.rocketmq.dashboard.service.ProxyAdminService.PopDiagnosticsResult;
import org.apache.rocketmq.dashboard.service.ProxyAdminService.ProxyAdminStatus;
import org.apache.rocketmq.dashboard.support.JsonResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Map;

/**
 * Proxy Admin REST API Controller for RIP-2 M2/M3/M4 operations.
 *
 * <p>Provides endpoints for proxy runtime management and diagnostics,
 * aggregating results from multiple Proxy Admin instances.</p>
 *
 * <h3>API Overview</h3>
 * <table border="1">
 *   <tr><th>Method</th><th>Path</th><th>Description</th></tr>
 *   <tr><td>GET</td><td>/api/proxy/admin/status</td><td>Get proxy admin connection status</td></tr>
 *   <tr><td>GET</td><td>/api/proxy/admin/config</td><td>Get runtime config from all proxies</td></tr>
 *   <tr><td>POST</td><td>/api/proxy/admin/config</td><td>Hot-update config on all proxies</td></tr>
 *   <tr><td>POST</td><td>/api/proxy/admin/disconnect/{clientId}</td><td>Force disconnect a client</td></tr>
 *   <tr><td>GET</td><td>/api/proxy/admin/pop-diagnostics</td><td>POP receipt handle diagnostics</td></tr>
 *   <tr><td>GET</td><td>/api/proxy/admin/batch-diagnostics</td><td>Batch consumption diagnostics</td></tr>
 * </table>
 */
@Controller
@RequestMapping("/api/proxy/admin")
public class ProxyAdminController {

    private static final Logger log = LoggerFactory.getLogger(ProxyAdminController.class);

    @Autowired
    private ProxyAdminService proxyAdminService;

    // ==================== Status ====================

    /**
     * GET /api/proxy/admin/status - Get proxy admin connection status.
     */
    @GetMapping("/status")
    @ResponseBody
    public Object getStatus() {
        try {
            ProxyAdminStatus status = proxyAdminService.getStatus();
            return new JsonResult<>(status);
        } catch (Exception e) {
            log.error("Failed to get proxy admin status", e);
            return new JsonResult<>(1, "Failed to get status: " + e.getMessage());
        }
    }

    // ==================== M2: Config Management ====================

    /**
     * GET /api/proxy/admin/config - Get runtime configuration from all proxy instances.
     */
    @GetMapping("/config")
    @ResponseBody
    public Object getConfig() {
        try {
            Map<String, Map<String, Object>> configs = proxyAdminService.getProxyConfigs();
            return new JsonResult<>(configs);
        } catch (Exception e) {
            log.error("Failed to get proxy configs", e);
            return new JsonResult<>(1, "Failed to get configs: " + e.getMessage());
        }
    }

    /**
     * POST /api/proxy/admin/config - Hot-update runtime configuration on all proxy instances.
     *
     * @param configUpdates map of field name → new value
     */
    @PostMapping("/config")
    @ResponseBody
    public Object updateConfig(@RequestBody Map<String, Object> configUpdates) {
        try {
            if (configUpdates == null || configUpdates.isEmpty()) {
                return new JsonResult<>(1, "Config updates cannot be empty");
            }

            Map<String, List<String>> results = proxyAdminService.updateProxyConfig(configUpdates);
            return new JsonResult<>(results);
        } catch (Exception e) {
            log.error("Failed to update proxy configs", e);
            return new JsonResult<>(1, "Failed to update configs: " + e.getMessage());
        }
    }

    // ==================== M2: Connection Management ====================

    /**
     * POST /api/proxy/admin/disconnect/{clientId} - Force disconnect a client.
     *
     * @param clientId the unique client identifier
     * @param reason   optional reason for audit logging
     */
    @PostMapping("/disconnect/{clientId}")
    @ResponseBody
    public Object disconnectClient(@PathVariable String clientId,
                                    @RequestParam(required = false, defaultValue = "Dashboard admin action") String reason) {
        try {
            if (clientId == null || clientId.trim().isEmpty()) {
                return new JsonResult<>(1, "Client ID cannot be empty");
            }

            boolean disconnected = proxyAdminService.disconnectClient(clientId.trim(), reason);
            if (disconnected) {
                return new JsonResult<>(Map.of("disconnected", true, "clientId", clientId));
            } else {
                return new JsonResult<>(1, "Client '" + clientId + "' not found or disconnect failed");
            }
        } catch (Exception e) {
            log.error("Failed to disconnect client {}", clientId, e);
            return new JsonResult<>(1, "Failed to disconnect: " + e.getMessage());
        }
    }

    // ==================== M3: POP Diagnostics ====================

    /**
     * GET /api/proxy/admin/pop-diagnostics - Query POP receipt handle diagnostics.
     *
     * @param group    consumer group name (required)
     * @param topic    optional topic filter
     * @param pageNum  page number (default 1)
     * @param pageSize page size (default 20)
     */
    @GetMapping("/pop-diagnostics")
    @ResponseBody
    public Object describePopReceiptHandles(
            @RequestParam String group,
            @RequestParam(required = false) String topic,
            @RequestParam(defaultValue = "1") int pageNum,
            @RequestParam(defaultValue = "20") int pageSize) {
        try {
            if (group == null || group.trim().isEmpty()) {
                return new JsonResult<>(1, "Consumer group name is required");
            }

            PopDiagnosticsResult result = proxyAdminService.describePopReceiptHandles(
                group.trim(), topic, pageNum, pageSize);
            return new JsonResult<>(result);
        } catch (Exception e) {
            log.error("Failed to describe POP receipt handles for group {}", group, e);
            return new JsonResult<>(1, "Failed to get POP diagnostics: " + e.getMessage());
        }
    }

    // ==================== M4: Batch Consumption Diagnostics ====================

    /**
     * GET /api/proxy/admin/batch-diagnostics - Query batch consumption diagnostics.
     *
     * @param group    consumer group name (required)
     * @param topic    optional topic filter
     * @param clientId optional client ID filter
     * @param pageNum  page number (default 1)
     * @param pageSize page size (default 20)
     */
    @GetMapping("/batch-diagnostics")
    @ResponseBody
    public Object describeBatchConsumeDiagnostics(
            @RequestParam String group,
            @RequestParam(required = false) String topic,
            @RequestParam(required = false) String clientId,
            @RequestParam(defaultValue = "1") int pageNum,
            @RequestParam(defaultValue = "20") int pageSize) {
        try {
            if (group == null || group.trim().isEmpty()) {
                return new JsonResult<>(1, "Consumer group name is required");
            }

            BatchConsumeDiagnosticsResult result = proxyAdminService.describeBatchConsumeDiagnostics(
                group.trim(), topic, clientId, pageNum, pageSize);
            return new JsonResult<>(result);
        } catch (Exception e) {
            log.error("Failed to describe batch consume diagnostics for group {}", group, e);
            return new JsonResult<>(1, "Failed to get batch diagnostics: " + e.getMessage());
        }
    }
}
