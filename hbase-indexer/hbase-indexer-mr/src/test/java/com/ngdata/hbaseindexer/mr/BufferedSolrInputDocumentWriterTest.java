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

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.ngdata.hbaseindexer.indexer.SolrInputDocumentWriter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

public class BufferedSolrInputDocumentWriterTest {

    private SolrInputDocumentWriter delegateWriter;
    private BufferedSolrInputDocumentWriter bufferedWriter;
    private Counter docCounter;
    private Counter docBatchCounter;

    @Before
    public void setUp() {
        delegateWriter = mock(SolrInputDocumentWriter.class);
        docCounter = mock(Counter.class);
        docBatchCounter = mock(Counter.class);
        bufferedWriter = new BufferedSolrInputDocumentWriter(delegateWriter, 3, docCounter, docBatchCounter);
    }


    @Test
    public void testAdd() throws SolrServerException, IOException {
        SolrInputDocument doc = mock(SolrInputDocument.class);
        bufferedWriter.add(-1, ImmutableMap.of("a", doc));

        verify(delegateWriter, never()).add(eq(-1), anyMap());
    }

    @Test
    public void testAdd_PastFlush() throws SolrServerException, IOException {
        SolrInputDocument docA = mock(SolrInputDocument.class);
        SolrInputDocument docB = mock(SolrInputDocument.class);
        SolrInputDocument docC = mock(SolrInputDocument.class);

        bufferedWriter.add(-1, ImmutableMap.of("a", docA));
        bufferedWriter.add(-1, ImmutableMap.of("b", docB));
        bufferedWriter.add(-1, ImmutableMap.of("c", docC));

        verify(delegateWriter).add(eq(-1), eq(ImmutableMap.of("a", docA, "b", docB, "c", docC)));
    }

    @Test
    public void testDeleteById() throws SolrServerException, IOException {
        bufferedWriter.deleteById(-1, ImmutableList.of("a"));

        verify(delegateWriter).deleteById(eq(-1), eq(ImmutableList.of("a")));
    }

    @Test
    public void testDeleteByQuery() throws SolrServerException, IOException {
        bufferedWriter.deleteByQuery("name:x");

        verify(delegateWriter).deleteByQuery("name:x");
    }

    @Test
    public void testFlush() throws SolrServerException, IOException {
        SolrInputDocument docA = mock(SolrInputDocument.class);
        SolrInputDocument docB = mock(SolrInputDocument.class);

        bufferedWriter.add(-1, ImmutableMap.of("a", docA));
        bufferedWriter.flush();

        verify(delegateWriter).add(eq(-1), eq(ImmutableMap.of("a", docA)));

        bufferedWriter.add(-1, ImmutableMap.of("b", docB));
        bufferedWriter.flush();

        verify(delegateWriter).add(eq(-1), eq(ImmutableMap.of("b", docB)));
        verify(docCounter, times(2)).increment(1L);
    }

    @Test
    public void testClose() throws SolrServerException, IOException {
        SolrInputDocument doc = mock(SolrInputDocument.class);
        bufferedWriter.add(-1, ImmutableMap.of("a", doc));

        bufferedWriter.close();

        verify(delegateWriter).add(eq(-1), eq(ImmutableMap.of("a", doc)));
        verify(delegateWriter).close();
        verify(docCounter).increment(1L);
    }

}
