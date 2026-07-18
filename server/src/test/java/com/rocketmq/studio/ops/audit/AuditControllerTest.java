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
package com.rocketmq.studio.ops.audit;

import com.rocketmq.studio.common.domain.PageResult;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AuditController.class)
@AutoConfigureMockMvc(addFilters = false)
class AuditControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @org.springframework.boot.test.mock.bean.MockBean
    private AuditService auditService;

    @Test
    void queryLogsShouldReturnPagedResults() throws Exception {
        AuditRecordVO record = AuditRecordVO.builder().operation("CREATE_TOPIC").build();
        PageResult<AuditRecordVO> pageResult = PageResult.of(List.of(record), 1L, 1, 20);
        when(auditService.queryLogs(anyInt(), anyInt(), anyString(), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(pageResult);

        mockMvc.perform(get("/api/audit-logs")
                        .param("page", "1")
                        .param("pageSize", "20"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.items[0].operation").value("CREATE_TOPIC"));
    }

    @Test
    void queryLogsWithFiltersShouldReturnFilteredResults() throws Exception {
        PageResult<AuditRecordVO> pageResult = PageResult.of(Collections.emptyList(), 0L, 1, 20);
        when(auditService.queryLogs(anyInt(), anyInt(), anyString(), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(pageResult);

        mockMvc.perform(get("/api/audit-logs")
                        .param("page", "1")
                        .param("pageSize", "20")
                        .param("search", "topic")
                        .param("operationType", "CREATE")
                        .param("startDate", "2024-01-01")
                        .param("endDate", "2024-12-31")
                        .param("result", "success"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.items").isEmpty());
    }

    @Test
    void cleanupLogsShouldReturnDeletedCount() throws Exception {
        when(auditService.cleanupLogs(30)).thenReturn(5);

        mockMvc.perform(post("/api/audit-logs/cleanup")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"beforeDays\":30}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.deleted").value(5));
    }

    @Test
    void cleanupLogsWithDefaultDaysShouldWork() throws Exception {
        when(auditService.cleanupLogs(30)).thenReturn(10);

        mockMvc.perform(post("/api/audit-logs/cleanup")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.deleted").value(10));
    }
}