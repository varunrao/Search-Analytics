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
package com.ngdata.hbaseindexer.parse;

import static com.ngdata.sep.impl.HBaseShims.newResult;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.NavigableSet;

import javax.xml.parsers.ParserConfigurationException;

import org.mockito.ArgumentCaptor;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ngdata.hbaseindexer.conf.DocumentExtractDefinition;
import com.ngdata.hbaseindexer.conf.FieldDefinition;
import com.ngdata.hbaseindexer.conf.FieldDefinition.ValueSource;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.core.SolrConfig;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class DefaultResultToSolrMapperTest {

    private SolrUpdateWriter solrUpdateWriter;
    private ArgumentCaptor<SolrInputDocument> solrInputDocCaptor;
    
    private static final byte[] ROW = Bytes.toBytes("row");
    private static final byte[] COLUMN_FAMILY_A = Bytes.toBytes("cfA");
    private static final byte[] COLUMN_FAMILY_B = Bytes.toBytes("cfB");
    private static final byte[] QUALIFIER_A = Bytes.toBytes("qualifierA");
    private static final byte[] QUALIFIER_B = Bytes.toBytes("qualifierB");

    @BeforeClass
    public static void setupBeforeClass() throws ParserConfigurationException, IOException, SAXException {
    }
    
    @Before
    public void setUp() {
        solrUpdateWriter = mock(SolrUpdateWriter.class);
        solrInputDocCaptor = ArgumentCaptor.forClass(SolrInputDocument.class);
    }

    @Test
    public void testMap() {
        FieldDefinition fieldDefA = new FieldDefinition("fieldA", "cfA:qualifierA", ValueSource.VALUE, "int");
        FieldDefinition fieldDefB = new FieldDefinition("fieldB", "cfB:qualifierB", ValueSource.VALUE,
                DummyValueMapper.class.getName());
        DefaultResultToSolrMapper resultMapper = new DefaultResultToSolrMapper("index-name", Lists.newArrayList(fieldDefA, fieldDefB),
                Collections.<DocumentExtractDefinition> emptyList());

        KeyValue kvA = new KeyValue(ROW, COLUMN_FAMILY_A, QUALIFIER_A, Bytes.toBytes(42));
        KeyValue kvB = new KeyValue(ROW, COLUMN_FAMILY_B, QUALIFIER_B, Bytes.toBytes("dummy value"));
        Result result = newResult(Lists.newArrayList(kvA, kvB));

        resultMapper.map(result, solrUpdateWriter);
        verify(solrUpdateWriter).add(solrInputDocCaptor.capture());
        
        SolrInputDocument solrDocument = solrInputDocCaptor.getValue();

        assertEquals(Sets.newHashSet("fieldA", "fieldB"), solrDocument.keySet());

        SolrInputField fieldA = solrDocument.get("fieldA");
        SolrInputField fieldB = solrDocument.get("fieldB");

        assertEquals(Lists.newArrayList(42), fieldA.getValues());
        assertEquals(Lists.newArrayList("A", "B", "C"), fieldB.getValues());
    }

    @Test
    public void testIsRelevantKV_WithoutWildcards() {
        FieldDefinition fieldDef = new FieldDefinition("fieldA", "cf:qualifier", ValueSource.VALUE, "int");
        DefaultResultToSolrMapper resultMapper = new DefaultResultToSolrMapper("index-name", Lists.newArrayList(fieldDef),
                Collections.<DocumentExtractDefinition> emptyList());

        KeyValue relevantKV = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("qualifier"),
                Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongFamily = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("wrongcf"),
                Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongQualifier = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"),
                Bytes.toBytes("wrongqualifier"), Bytes.toBytes("value"));

        assertTrue(resultMapper.isRelevantKV(relevantKV));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongFamily));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongQualifier));
    }

    @Test
    public void testIsRelevantKV_WithWildcards() {
        FieldDefinition fieldDef = new FieldDefinition("fieldA", "cf:quali*", ValueSource.VALUE, "int");
        DefaultResultToSolrMapper resultMapper = new DefaultResultToSolrMapper("index-name", Lists.newArrayList(fieldDef),
                Collections.<DocumentExtractDefinition> emptyList());

        KeyValue relevantKV = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("qualifier"),
                Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongFamily = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("wrongcf"),
                Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongQualifier = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"),
                Bytes.toBytes("qu wrong qualifier"), Bytes.toBytes("value"));

        assertTrue(resultMapper.isRelevantKV(relevantKV));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongFamily));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongQualifier));
    }
    
    @Test
    public void testGetGet_SingleCellFieldDefinition() {
        FieldDefinition fieldDef = new FieldDefinition("fieldname", "cf:qualifier", ValueSource.VALUE, "int");
        
        DefaultResultToSolrMapper resultMapper = new DefaultResultToSolrMapper("index-name", Lists.newArrayList(fieldDef),
                        Collections.<DocumentExtractDefinition>emptyList());
        Get get = resultMapper.getGet(ROW);
        
        assertArrayEquals(ROW, get.getRow());
        assertEquals(1, get.getFamilyMap().size());
        
        assertTrue(get.getFamilyMap().containsKey(Bytes.toBytes("cf")));
        NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(Bytes.toBytes("cf"));
        assertEquals(1, qualifiers.size());
        assertTrue(qualifiers.contains(Bytes.toBytes("qualifier")));
    }
    
    @Test
    public void testGetGet_WildcardFieldDefinition() {
        FieldDefinition fieldDef = new FieldDefinition("fieldname", "cf:qual*", ValueSource.VALUE, "int");
        
        DefaultResultToSolrMapper resultMapper = new DefaultResultToSolrMapper("index-name", Lists.newArrayList(fieldDef),
                        Collections.<DocumentExtractDefinition>emptyList());
        Get get = resultMapper.getGet(ROW);
        
        assertArrayEquals(ROW, get.getRow());
        assertEquals(1, get.getFamilyMap().size());
        
        assertTrue(get.getFamilyMap().containsKey(Bytes.toBytes("cf")));
        NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(Bytes.toBytes("cf"));
        assertNull(qualifiers);
    }
    
    @Test
    public void testContainsRequiredData_True() {
        DefaultResultToSolrMapper resultToSolrMapper = new DefaultResultToSolrMapper("index-name",
                Lists.newArrayList(new FieldDefinition("fieldname", "cfA:qualifierA", ValueSource.VALUE, "int")),
                        Collections.<DocumentExtractDefinition>emptyList());
        
        Result result = newResult(Lists.newArrayList(new KeyValue(ROW, COLUMN_FAMILY_A, QUALIFIER_A, Bytes.toBytes("value"))));
        
        assertTrue(resultToSolrMapper.containsRequiredData(result));
    }
    
    @Test
    public void testContainsRequiredData_False() {
        // With a wildcard we can never know if a Result contains all required data to perform indexing
        DefaultResultToSolrMapper resultToSolrMapper = new DefaultResultToSolrMapper("index-name",
                Lists.newArrayList(new FieldDefinition("fieldname", "cfA:quali*", ValueSource.VALUE, "int")),
                        Collections.<DocumentExtractDefinition>emptyList());
        
        Result result = newResult(Lists.newArrayList(new KeyValue(ROW, COLUMN_FAMILY_A, QUALIFIER_A, Bytes.toBytes("value"))));
        
        assertFalse(resultToSolrMapper.containsRequiredData(result));
    }

    public static class DummyValueMapper implements ByteArrayValueMapper {

        @Override
        public Collection<Object> map(byte[] input) {
            return Lists.<Object> newArrayList("A", "B", "C");
        }

    }
}
