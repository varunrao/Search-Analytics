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
package com.ngdata.hbaseindexer.indexer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.Before;
import org.junit.Test;

public class IdAddingSolrUpdateWriterTest {

    private static final String UNIQUE_KEY_FIELD = "_unique_key_field_";
    private static final String DOCUMENT_ID = "_doc_id_";
    private static final String TABLE_NAME = "_table_";

    private SolrInputDocument solrDoc;
    private SolrUpdateCollector updateCollector;

    @Before
    public void setUp() {
        solrDoc = mock(SolrInputDocument.class);
        updateCollector = mock(SolrUpdateCollector.class);
        
    }

    @Test
    public void testAdd_AddId() {
        IdAddingSolrUpdateWriter updateWriter = new IdAddingSolrUpdateWriter(UNIQUE_KEY_FIELD, DOCUMENT_ID,
                null, TABLE_NAME, updateCollector);
        updateWriter.add(solrDoc);

        verify(solrDoc).addField(UNIQUE_KEY_FIELD, DOCUMENT_ID);
        verify(updateCollector).add(DOCUMENT_ID, solrDoc);
    }

    @Test
    public void testAdd_IdAlreadyPresent() {
        IdAddingSolrUpdateWriter updateWriter = new IdAddingSolrUpdateWriter(
                UNIQUE_KEY_FIELD, DOCUMENT_ID, null, TABLE_NAME, updateCollector);
        
        
        SolrInputField solrIdField = new SolrInputField(DOCUMENT_ID);
        solrIdField.setValue(DOCUMENT_ID, 1.0f);
        
        when(solrDoc.getField(UNIQUE_KEY_FIELD)).thenReturn(solrIdField);

        updateWriter.add(solrDoc);

        verify(updateCollector).add(DOCUMENT_ID, solrDoc);
    }

    // Adding two documents without ids to the same update writer isn't allowed because
    // it would only result in a single document in Solr
    @Test(expected = IllegalStateException.class)
    public void testAdd_MultipleDocumentsForOneId() {
        IdAddingSolrUpdateWriter updateWriter = new IdAddingSolrUpdateWriter(
                UNIQUE_KEY_FIELD, DOCUMENT_ID, null, TABLE_NAME, updateCollector);
        
        updateWriter.add(solrDoc);
        updateWriter.add(solrDoc);
    }
    
    @Test
    public void testAdd_MultipleDocumentsWithTheirOwnIds() {
        
        String idA = DOCUMENT_ID + "A";
        String idB = DOCUMENT_ID + "B";
        
        IdAddingSolrUpdateWriter updateWriter = new IdAddingSolrUpdateWriter(
                UNIQUE_KEY_FIELD, DOCUMENT_ID, null, TABLE_NAME, updateCollector);
        
        SolrInputDocument docA = mock(SolrInputDocument.class);
        SolrInputDocument docB = mock(SolrInputDocument.class);
        
        SolrInputField keyFieldA = new SolrInputField(UNIQUE_KEY_FIELD);
        keyFieldA.setValue(idA, 1.0f);
        SolrInputField keyFieldB = new SolrInputField(UNIQUE_KEY_FIELD);
        keyFieldB.setValue(idB, 1.0f);
        
        
        when(docA.getField(UNIQUE_KEY_FIELD)).thenReturn(keyFieldA);
        when(docB.getField(UNIQUE_KEY_FIELD)).thenReturn(keyFieldB);

        updateWriter.add(docA);
        updateWriter.add(docB);

        verify(updateCollector).add(idA, docA);
        verify(updateCollector).add(idB, docB);
    }
    
    @Test
    public void testAdd_IncludeTableName() {
        IdAddingSolrUpdateWriter updateWriter = new IdAddingSolrUpdateWriter(
                UNIQUE_KEY_FIELD, DOCUMENT_ID, "tableNameField", TABLE_NAME, updateCollector);
        
        updateWriter.add(solrDoc);

        verify(solrDoc).addField(UNIQUE_KEY_FIELD, DOCUMENT_ID);
        verify(solrDoc).addField("tableNameField", TABLE_NAME);
        verify(updateCollector).add(DOCUMENT_ID, solrDoc);
    }

    @Test
    public void testDeleteById() {
        IdAddingSolrUpdateWriter updateWriter = new IdAddingSolrUpdateWriter(UNIQUE_KEY_FIELD, DOCUMENT_ID,
                null, TABLE_NAME, updateCollector);
        updateWriter.deleteById(DOCUMENT_ID);

        verify(updateCollector).deleteById(DOCUMENT_ID);
    }

}
