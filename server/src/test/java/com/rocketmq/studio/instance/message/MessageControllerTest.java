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
package com.rocketmq.studio.instance.message;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(MessageController.class)
@AutoConfigureMockMvc(addFilters = false)
class MessageControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @org.springframework.boot.test.mock.bean.MockBean
    private MessageService messageService;

    @Test
    void queryMessagesShouldReturnMessages() throws Exception {
        MessageRecordVO record = MessageRecordVO.builder().msgId("msg-001").topic("test-topic").build();
        when(messageService.queryMessages(anyString(), anyString(), anyString(), any(), any()))
                .thenReturn(List.of(record));

        mockMvc.perform(get("/api/messages")
                        .param("topic", "test-topic")
                        .param("msgId", "msg-001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data[0].msgId").value("msg-001"));
    }

    @Test
    void queryMessagesShouldReturnEmptyList() throws Exception {
        when(messageService.queryMessages(anyString(), anyString(), anyString(), any(), any()))
                .thenReturn(Collections.emptyList());

        mockMvc.perform(get("/api/messages")
                        .param("topic", "nonexistent"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data").isEmpty());
    }

    @Test
    void getMessageTraceShouldReturnTrace() throws Exception {
        TraceNodeVO node = TraceNodeVO.builder().build();
        TraceRecordVO trace = TraceRecordVO.builder().msgId("msg-001").nodes(List.of(node)).build();
        when(messageService.getMessageTrace("msg-001")).thenReturn(trace);

        mockMvc.perform(get("/api/messages/msg-001/trace"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.msgId").value("msg-001"));
    }
}