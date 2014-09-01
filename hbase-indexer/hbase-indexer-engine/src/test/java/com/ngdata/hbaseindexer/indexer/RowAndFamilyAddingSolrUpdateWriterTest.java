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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.ngdata.hbaseindexer.parse.SolrUpdateWriter;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

public class RowAndFamilyAddingSolrUpdateWriterTest {
    
    private KeyValue keyValue;
    private UniqueKeyFormatter uniqueKeyFormatter;
    private SolrInputDocument solrDocument;
    private SolrUpdateWriter delegateWriter;

    @Before
    public void setUp() {
        keyValue = mock(KeyValue.class);
        uniqueKeyFormatter = mock(UniqueKeyFormatter.class);
        solrDocument = mock(SolrInputDocument.class);
        delegateWriter = mock(SolrUpdateWriter.class);
    }

    @Test
    public void testAdd_NoFields() {
        RowAndFamilyAddingSolrUpdateWriter updateWriter = new RowAndFamilyAddingSolrUpdateWriter(null, null,
                uniqueKeyFormatter, keyValue, delegateWriter);
        updateWriter.add(solrDocument);

        verify(solrDocument, never()).addField(anyString(), any());
        verify(delegateWriter).add(solrDocument);
    }

    @Test
    public void testAdd_ColumnField() {
        RowAndFamilyAddingSolrUpdateWriter updateWriter = new RowAndFamilyAddingSolrUpdateWriter(null, "_col_",
                uniqueKeyFormatter, keyValue, delegateWriter);
        
        doReturn(new byte[0]).when(keyValue).getFamily();
        doReturn("_famname_").when(uniqueKeyFormatter).formatFamily(any(byte[].class));
        updateWriter.add(solrDocument);
        
        verify(solrDocument).addField("_col_", "_famname_");
        verify(delegateWriter).add(solrDocument);
    }

    @Test
    public void testAdd_RowField() {
        RowAndFamilyAddingSolrUpdateWriter updateWriter = new RowAndFamilyAddingSolrUpdateWriter("_row_", null,
                uniqueKeyFormatter, keyValue, delegateWriter);
        
        byte[] rowBytes = Bytes.toBytes("_row_");
        
        doReturn(rowBytes).when(keyValue).getRow();
        doReturn("_rowkey_").when(uniqueKeyFormatter).formatRow(rowBytes);
        updateWriter.add(solrDocument);
        
        verify(solrDocument).addField("_row_", "_rowkey_");
        verify(delegateWriter).add(solrDocument);
    }

    @Test
    public void testDeleteById() {
        RowAndFamilyAddingSolrUpdateWriter updateWriter = new RowAndFamilyAddingSolrUpdateWriter("_row_", null,
                uniqueKeyFormatter, keyValue, delegateWriter);
        byte[] rowBytes = Bytes.toBytes("_row_");
        doReturn("_rowkey_").when(uniqueKeyFormatter).formatRow(rowBytes);
        updateWriter.deleteById(uniqueKeyFormatter.formatRow(rowBytes));

        verify(delegateWriter).deleteById("_rowkey_");
    }

}
