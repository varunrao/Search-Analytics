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
package com.ngdata.hbaseindexer.mr;

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class MapReduceSolrInputDocumentWriterTest {
    
    private Context context;
    private MapReduceSolrInputDocumentWriter mrWriter;
    
    @Before
    public void setUp() {
        context = mock(Context.class, Mockito.RETURNS_DEEP_STUBS);
        mrWriter = new MapReduceSolrInputDocumentWriter(context);
    }

    @Test
    public void testAdd() throws SolrServerException, IOException, InterruptedException {
        SolrInputDocument solrInputDoc = mock(SolrInputDocument.class);
        mrWriter.add(-1, ImmutableMap.of("docId", solrInputDoc));
        
        ArgumentCaptor<SolrInputDocumentWritable> docWritableCaptor =
                ArgumentCaptor.forClass(SolrInputDocumentWritable.class);
        
        verify(context).write(eq(new Text("docId")), docWritableCaptor.capture());
        
        assertSame(solrInputDoc, docWritableCaptor.getValue().getSolrInputDocument());
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testDeleteById() throws SolrServerException, IOException {
        mrWriter.deleteById(-1, ImmutableList.of("myId"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testDeleteByQuery() throws SolrServerException, IOException {
        mrWriter.deleteByQuery("*:*");
    }

}
