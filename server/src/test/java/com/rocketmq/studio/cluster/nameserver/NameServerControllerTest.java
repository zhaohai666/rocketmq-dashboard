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
package com.rocketmq.studio.cluster.nameserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rocketmq.studio.cluster.broker.ClusterService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(NameServerController.class)
@AutoConfigureMockMvc(addFilters = false)
class NameServerControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private ClusterService clusterService;

    @Test
    void createNameServerShouldReturnCreatedNameServer() throws Exception {
        NameServerVO vo = NameServerVO.builder()
                .id("ns-1")
                .name("nameserver-1")
                .build();
        when(clusterService.createNameServer(any(CreateNameServerDTO.class))).thenReturn(vo);

        CreateNameServerDTO dto = new CreateNameServerDTO();

        mockMvc.perform(post("/api/nameservers/create")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.id").value("ns-1"));
    }

    @Test
    void updateNameServerShouldReturnSuccess() throws Exception {
        doNothing().when(clusterService).updateNameServer(any(UpdateNameServerDTO.class));

        UpdateNameServerDTO dto = new UpdateNameServerDTO();

        mockMvc.perform(post("/api/nameservers/update")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200));
    }

    @Test
    void restartNameServerShouldReturnSuccessMap() throws Exception {
        when(clusterService.restartNameServer(any(RestartNameServerDTO.class))).thenReturn(true);

        RestartNameServerDTO dto = new RestartNameServerDTO();

        mockMvc.perform(post("/api/nameservers/restart")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.success").value(true));
    }

    @Test
    void upgradeNameServerShouldReturnSuccessMap() throws Exception {
        when(clusterService.upgradeNameServer(any(UpgradeNameServerDTO.class))).thenReturn(true);

        UpgradeNameServerDTO dto = new UpgradeNameServerDTO();

        mockMvc.perform(post("/api/nameservers/upgrade")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.success").value(true));
    }

    @Test
    void deleteNameServerShouldReturnSuccessMap() throws Exception {
        when(clusterService.deleteNameServer(any(DeleteNameServerDTO.class))).thenReturn(true);

        DeleteNameServerDTO dto = new DeleteNameServerDTO();

        mockMvc.perform(post("/api/nameservers/delete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.success").value(true));
    }

    @Test
    void deleteNameServerShouldReturnFailureMap() throws Exception {
        when(clusterService.deleteNameServer(any(DeleteNameServerDTO.class))).thenReturn(false);

        DeleteNameServerDTO dto = new DeleteNameServerDTO();

        mockMvc.perform(post("/api/nameservers/delete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.success").value(false));
    }
}