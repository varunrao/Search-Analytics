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
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class BasePayloadExtractorTest {

    private static final byte[] TABLE_NAME = Bytes.toBytes("table_name");
    private static final byte[] ROW = Bytes.toBytes("row_key");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("column_family");
    private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("column_qualifier");
    private static final byte[] PAYLOAD_DATA = Bytes.toBytes("payload");

    private BasePayloadExtractor extractor;

    @Before
    public void setUp() {
        extractor = new BasePayloadExtractor(TABLE_NAME, COLUMN_FAMILY, COLUMN_QUALIFIER);
    }

    @Test
    public void testExtractPayload_PayloadIncluded() {
        byte[] payload = extractor.extractPayload(TABLE_NAME, new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER,
                PAYLOAD_DATA));
        assertArrayEquals(PAYLOAD_DATA, payload);
    }

    @Test
    public void testExtractPayload_WrongTable() {
        byte[] payload = extractor.extractPayload(Bytes.toBytes("wrong_table"), new KeyValue(ROW, COLUMN_FAMILY,
                COLUMN_QUALIFIER, PAYLOAD_DATA));
        assertNull(payload);
    }

    @Test
    public void testExtractPayload_WrongColumnFamily() {
        byte[] payload = extractor.extractPayload(TABLE_NAME, new KeyValue(ROW, Bytes.toBytes("wrong family"),
                COLUMN_QUALIFIER, PAYLOAD_DATA));
        assertNull(payload);
    }

    @Test
    public void testExtractPayload_WrongColumnQualifier() {
        byte[] payload = extractor.extractPayload(TABLE_NAME,
                new KeyValue(ROW, COLUMN_FAMILY, Bytes.toBytes("wrong qualifier"), PAYLOAD_DATA));
        assertNull(payload);
    }

}
