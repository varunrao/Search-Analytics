/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.sep.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class HBaseEventPublisherTest {
    
    private static final byte[] PAYLOAD_CF = Bytes.toBytes("payload column family");
    private static final byte[] PAYLOAD_CQ = Bytes.toBytes("payload column qualifier");

    private HTableInterface recordTable;
    private HBaseEventPublisher eventPublisher;

    @Before
    public void setUp() {
        recordTable = mock(HTableInterface.class);
        eventPublisher = new HBaseEventPublisher(recordTable, PAYLOAD_CF, PAYLOAD_CQ);
    }

    @Test
    public void testPublishEvent() throws IOException {
        byte[] eventRow = Bytes.toBytes("row-id");
        byte[] eventPayload = Bytes.toBytes("payload");
        
        ArgumentCaptor<Put> putCaptor = ArgumentCaptor.forClass(Put.class);
        
        eventPublisher.publishEvent(eventRow, eventPayload);
        
        verify(recordTable).put(putCaptor.capture());
        Put put = putCaptor.getValue();
        
        assertArrayEquals(eventRow, put.getRow());
        assertEquals(1, put.size());
        assertArrayEquals(eventPayload, put.get(PAYLOAD_CF, PAYLOAD_CQ).get(0).getValue());
    }

}
