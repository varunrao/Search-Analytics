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

import com.ngdata.hbaseindexer.parse.SolrUpdateWriter;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.solr.common.SolrInputDocument;

/**
 * SolrUpdateWriter decorator that adds row and colument family information to Solr documents if that has been
 * configured.
 */
public class RowAndFamilyAddingSolrUpdateWriter implements SolrUpdateWriter {

    private final String rowField;
    private final String columnFamilyField;
    private final KeyValue keyValue;
    private final UniqueKeyFormatter uniqueKeyFormatter;
    private final SolrUpdateWriter delegateUpdateWriter;

    /**
     * Instantiate with the row and column field names to be used in Solr.
     * 
     * @param rowField name of the field in Solr used to store the row value
     * @param columnFamilyField name of the field in Solr used to store the column name
     * @param uniqueKeyFormatter key formatter for converting KeyValue identifying information into Solr fields
     * @param keyValue KeyValue being indexed
     * @param delegateUpdateWriter update writer to which decorated Solr document are written
     */
    public RowAndFamilyAddingSolrUpdateWriter(String rowField, String columnFamilyField,
            UniqueKeyFormatter uniqueKeyFormatter,
            KeyValue keyValue,SolrUpdateWriter delegateUpdateWriter) {
        this.rowField = rowField;
        this.columnFamilyField = columnFamilyField;
        this.uniqueKeyFormatter = uniqueKeyFormatter;
        this.keyValue = keyValue;
        this.delegateUpdateWriter = delegateUpdateWriter;
    }

    @Override
    public void add(SolrInputDocument solrDocument) {
        if (rowField != null) {
            solrDocument.addField(rowField, uniqueKeyFormatter.formatRow(keyValue.getRow()));
        }
        
        if (columnFamilyField != null) {
            solrDocument.addField(columnFamilyField, uniqueKeyFormatter.formatFamily(keyValue.getFamily()));
        }
        
        delegateUpdateWriter.add(solrDocument);
    }

    @Override
    public void deleteById(String documentId) {
        delegateUpdateWriter.deleteById(documentId);
    }

    @Override
    public void deleteByQuery(String query) {
        delegateUpdateWriter.deleteByQuery(query);
    }
}
