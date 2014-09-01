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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.SolrInputDocumentWritable;

import com.ngdata.hbaseindexer.indexer.SolrInputDocumentWriter;

/**
 * Writer for {@code SolrInputDocument}s in a MapReduce context.
 */
class MapReduceSolrInputDocumentWriter implements SolrInputDocumentWriter {
    
    private Context context;
    
    public MapReduceSolrInputDocumentWriter(Context context) {
        this.context = context;
    }

    @Override
    public void add(int shard, Map<String, SolrInputDocument> inputDocumentMap) throws SolrServerException, IOException {
        for (Entry<String, SolrInputDocument> documentEntry : inputDocumentMap.entrySet()) {
            try {
                context.write(
                    new Text(documentEntry.getKey()),
                    new SolrInputDocumentWritable(documentEntry.getValue()));
                context.getCounter(HBaseIndexerCounters.OUTPUT_INDEX_DOCUMENTS).increment(1L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void deleteById(int shard, List<String> idsToDelete) throws SolrServerException, IOException {
        throw new UnsupportedOperationException("Cannot delete records in a MapReduce context");
    }

    @Override
    public void deleteByQuery(String deleteQuery) throws SolrServerException, IOException {
        throw new UnsupportedOperationException("Cannot delete records in a MapReduce context");
    }
    
    @Override
    public void close() throws SolrServerException, IOException {
        // Nothing to do
    }

}
