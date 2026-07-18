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
package com.rocketmq.studio.cluster.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(MetricsController.class)
@AutoConfigureMockMvc(addFilters = false)
class MetricsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private MetricsService metricsService;

    @Test
    void queryShouldReturnMetricData() throws Exception {
        MetricDataVO data = MetricDataVO.builder()
                .metric("rocketmq_broker_tps")
                .values(Arrays.asList(new long[]{1000, 5000}, new long[]{2000, 6000}))
                .build();

        when(metricsService.query(any(MetricQueryDTO.class))).thenReturn(data);

        MetricQueryDTO query = MetricQueryDTO.builder()
                .metric("rocketmq_broker_tps")
                .start(1000L)
                .end(2000L)
                .step("15s")
                .build();

        mockMvc.perform(post("/api/metrics/query")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(query)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.metric").value("rocketmq_broker_tps"));
    }

    @Test
    void queryShouldReturnEmptyData() throws Exception {
        MetricDataVO data = MetricDataVO.builder()
                .metric("rocketmq_broker_tps")
                .values(null)
                .build();

        when(metricsService.query(any(MetricQueryDTO.class))).thenReturn(data);

        MetricQueryDTO query = MetricQueryDTO.builder()
                .metric("rocketmq_broker_tps")
                .start(1000L)
                .end(2000L)
                .step("15s")
                .build();

        mockMvc.perform(post("/api/metrics/query")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(query)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.metric").value("rocketmq_broker_tps"));
    }
}