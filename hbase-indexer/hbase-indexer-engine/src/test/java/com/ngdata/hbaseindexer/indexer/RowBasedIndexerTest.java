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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.IndexerConf.MappingType;
import com.ngdata.hbaseindexer.conf.IndexerConfBuilder;
import com.ngdata.hbaseindexer.indexer.Indexer.RowBasedIndexer;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

public class RowBasedIndexerTest {
    
    private static final String TABLE_NAME = "TABLE_A";
    
    private IndexerConf indexerConf;
    private HTablePool tablePool;
    private SolrInputDocumentWriter solrWriter;
    private SolrUpdateCollector updateCollector;
    private RowBasedIndexer indexer;
    
    @Before
    public void setUp() {
        
        indexerConf = spy(new IndexerConfBuilder().table(TABLE_NAME).mappingType(MappingType.ROW).build());
        ResultToSolrMapper mapper = IndexingEventListenerTest.createHbaseToSolrMapper(true);
        
        tablePool = mock(HTablePool.class);
        solrWriter = mock(DirectSolrInputDocumentWriter.class);
        
        updateCollector = new SolrUpdateCollector(10);
        
        indexer = new RowBasedIndexer("row-based", indexerConf, TABLE_NAME, mapper, tablePool, null, solrWriter);
    }
    
    private RowData createEventRowData(String row, KeyValue... keyValues) {
        return new SepEventRowData(
                new SepEvent(Bytes.toBytes(TABLE_NAME),
                       Bytes.toBytes(row), Lists.newArrayList(keyValues), null));
    }

    @Test
    public void testCalculateIndexUpdates_AddDocument() throws IOException {
        
        KeyValue keyValue = new KeyValue(Bytes.toBytes("_row_"), Bytes.toBytes("_cf_"),
                                         Bytes.toBytes("_qual_"), Bytes.toBytes("value"));
        RowData rowData = createEventRowData("_row_", keyValue);
        indexer.calculateIndexUpdates(ImmutableList.of(rowData), updateCollector);
        
        assertEquals(1, updateCollector.getDocumentsToAdd().size());
        assertEquals("_row_", updateCollector.getDocumentsToAdd().get("_row_").getFieldValue("id"));
        assertTrue(updateCollector.getIdsToDelete().isEmpty());
    }
    
    @Test
    public void testCalculateIndexUpdates_AddDocumentWithTableName() throws IOException {
        
        doReturn("custom-table-name").when(indexerConf).getTableNameField();
        
        KeyValue keyValue = new KeyValue(Bytes.toBytes("_row_"), Bytes.toBytes("_cf_"),
                                         Bytes.toBytes("_qual_"),
                                         Bytes.toBytes("value"));
        RowData rowData = createEventRowData("_row_", keyValue);
        indexer.calculateIndexUpdates(ImmutableList.of(rowData), updateCollector);
        
        assertEquals(1, updateCollector.getDocumentsToAdd().size());
        List<SolrInputDocument> documents = Lists.newArrayList(updateCollector.getDocumentsToAdd().values());
        assertEquals(TABLE_NAME, documents.get(0).getFieldValue("custom-table-name"));
    }
    
    @Test
    public void testCalculateIndexUpdates_DeleteCell() throws IOException {
        
        KeyValue keyValue = new KeyValue(Bytes.toBytes("_row_"), Bytes.toBytes("_cf_"),
                                         Bytes.toBytes("_qual_"),
                                         0L,
                                         Type.DeleteColumn);
        RowData rowData = createEventRowData("_row_", keyValue);
        indexer.calculateIndexUpdates(ImmutableList.of(rowData), updateCollector);
        
        assertEquals(Lists.newArrayList("_row_"), updateCollector.getIdsToDelete());
        assertTrue(updateCollector.getDocumentsToAdd().isEmpty());
    }
    
    @Test
    public void testCalculateIndexUpdates_DeleteRow() throws IOException {
        
        KeyValue keyValue = new KeyValue(Bytes.toBytes("_row_"), Bytes.toBytes(""), Bytes.toBytes(""), 0L,
                                         Type.Delete);
        RowData rowData = createEventRowData("_row_", keyValue);
        indexer.calculateIndexUpdates(ImmutableList.of(rowData), updateCollector);
        
        assertEquals(Lists.newArrayList("_row_"), updateCollector.getIdsToDelete());
        assertTrue(updateCollector.getDocumentsToAdd().isEmpty());
    }
    
    @Test
    public void testCalculateIndexUpdates_UpdateAndDeleteCombinedForSameCell_DeleteFirst() throws IOException {
        KeyValue toDelete = new KeyValue(Bytes.toBytes("_row_"), Bytes.toBytes("_cf_"),
                                         Bytes.toBytes("_qual_"),
                                         0L,
                                         Type.Delete);
        KeyValue toAdd = new KeyValue(Bytes.toBytes("_row_"), Bytes.toBytes("_cf_"),
                                      Bytes.toBytes("_qual_"),
                                      Bytes.toBytes("value"));
        RowData deleteEventRowData = createEventRowData("_row_", toDelete);
        RowData addEventRowData = createEventRowData("_row_", toAdd);

        indexer.calculateIndexUpdates(ImmutableList.of(deleteEventRowData, addEventRowData), updateCollector);

        assertTrue(updateCollector.getIdsToDelete().isEmpty());
        List<SolrInputDocument> documents = Lists.newArrayList(updateCollector.getDocumentsToAdd().values());
        assertEquals(1, documents.size());
        assertEquals("_row_", documents.get(0).getFieldValue("id"));
    }

    @Test
    public void testCalculateIndexUpdates_UpdateAndDeleteCombinedForSameCell_UpdateFirst() throws IOException {
        KeyValue toAdd = new KeyValue(Bytes.toBytes("_row_"), Bytes.toBytes("_cf_"),
                                      Bytes.toBytes("_qual_"),
                                      Bytes.toBytes("value"));
        KeyValue toDelete = new KeyValue(Bytes.toBytes("_row_"), Bytes.toBytes("_cf_"),
                                         Bytes.toBytes("_qual_"),
                                         0L,
                                         Type.Delete);
        RowData addEventRowData = createEventRowData("_row_", toAdd);
        RowData deleteEventRowData = createEventRowData("_row_", toDelete);

        indexer.calculateIndexUpdates(Lists.newArrayList(addEventRowData, deleteEventRowData), updateCollector);

        assertEquals(Lists.newArrayList("_row_"), updateCollector.getIdsToDelete());
        assertTrue(updateCollector.getDocumentsToAdd().isEmpty());
    }

}
