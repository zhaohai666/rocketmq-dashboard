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
package com.rocketmq.studio.ops.alert;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(SystemAlertController.class)
@AutoConfigureMockMvc(addFilters = false)
class SystemAlertControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @org.springframework.boot.test.mock.bean.MockBean
    private AlertService alertService;

    @Test
    void listAlertsShouldReturnAllAlerts() throws Exception {
        SystemAlertVO alert = SystemAlertVO.builder().message("Disk full").build();
        when(alertService.listAlerts(anyString())).thenReturn(List.of(alert));

        mockMvc.perform(get("/api/system-alerts")
                        .param("level", "error"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data[0].message").value("Disk full"));
    }

    @Test
    void listAlertsShouldReturnEmptyList() throws Exception {
        when(alertService.listAlerts(anyString())).thenReturn(Collections.emptyList());

        mockMvc.perform(get("/api/system-alerts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data").isEmpty());
    }

    @Test
    void acknowledgeAlertShouldReturnAcknowledgedAlert() throws Exception {
        SystemAlertVO alert = SystemAlertVO.builder().message("Acknowledged").build();
        when(alertService.acknowledgeAlert("alert-1")).thenReturn(alert);

        mockMvc.perform(post("/api/system-alerts/acknowledge")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"alert-1\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.message").value("Acknowledged"));
    }

    @Test
    void clearAcknowledgedShouldReturnClearedCount() throws Exception {
        when(alertService.clearAcknowledged()).thenReturn(3);

        mockMvc.perform(post("/api/system-alerts/clear-acknowledged"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.cleared").value(3));
    }
}