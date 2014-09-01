/*
 * Copyright 2013 NGDATA nv
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

package com.ngdata.hbaseindexer.conf;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexerConfTest {

    @Test
    public void testGetTable_BasicLiteral() throws Exception {
        assertEquals("myTableName", new IndexerConf("myTableName").getTable());
    }

    @Test
    public void testGetTable_QualifiedLiteral() {
        assertEquals("myTableName", new IndexerConf("literal:myTableName").getTable());
    }

    @Test
    public void testGetTable_Regex() {
        assertEquals("my.*Name", new IndexerConf("regex:my.*Name").getTable());
    }

    @Test
    public void testTableNameIsRegex_BasicLiteral() throws Exception {
        assertFalse(new IndexerConf("myTableName").tableNameIsRegex());
    }

    @Test
    public void testTableNameIsRegex_QualifiedLiteral() {
        assertFalse(new IndexerConf("literal:myTableName").tableNameIsRegex());
    }

    @Test
    public void testTableNameIsRegex_Regex() {
        assertTrue(new IndexerConf("regex:my.*Name").tableNameIsRegex());
    }
}
