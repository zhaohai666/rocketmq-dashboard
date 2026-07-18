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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AlertRuleController.class)
@AutoConfigureMockMvc(addFilters = false)
class AlertRuleControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @org.springframework.boot.test.mock.bean.MockBean
    private AlertService alertService;

    @Test
    void listRulesShouldReturnAllRules() throws Exception {
        AlertRuleVO rule = AlertRuleVO.builder().name("High CPU").build();
        when(alertService.listRules()).thenReturn(List.of(rule));

        mockMvc.perform(get("/api/alert-rules"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data[0].name").value("High CPU"));
    }

    @Test
    void createRuleShouldReturnCreatedRule() throws Exception {
        AlertRuleVO rule = AlertRuleVO.builder().name("New Rule").build();
        when(alertService.createRule(any(AlertRuleVO.class))).thenReturn(rule);

        mockMvc.perform(post("/api/alert-rules/create")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"New Rule\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.name").value("New Rule"));
    }

    @Test
    void updateRuleShouldReturnUpdatedRule() throws Exception {
        AlertRuleVO rule = AlertRuleVO.builder().name("Updated Rule").build();
        when(alertService.updateRule(any(AlertRuleVO.class))).thenReturn(rule);

        mockMvc.perform(post("/api/alert-rules/update")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Updated Rule\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.name").value("Updated Rule"));
    }

    @Test
    void toggleRuleShouldReturnToggledRule() throws Exception {
        AlertRuleVO rule = AlertRuleVO.builder().name("Toggled Rule").build();
        when(alertService.toggleRule(anyString(), anyBoolean())).thenReturn(rule);

        mockMvc.perform(post("/api/alert-rules/toggle")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"rule-1\",\"enabled\":true}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.name").value("Toggled Rule"));
    }

    @Test
    void deleteRuleShouldReturnSuccess() throws Exception {
        mockMvc.perform(post("/api/alert-rules/delete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"rule-1\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200));

        verify(alertService).deleteRule("rule-1");
    }
}