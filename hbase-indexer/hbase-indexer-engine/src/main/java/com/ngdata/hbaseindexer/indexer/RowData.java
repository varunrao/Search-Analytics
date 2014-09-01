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

import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

/**
 * Represents a collection of {@code KeyValue}s under a single row key. This can be seen as a generalization of an HBase
 * Result object.
 */
public interface RowData {

    /**
     * Get the HBase row key to which this data refers.
     * 
     * @return the row key bytes
     */
    byte[] getRow();

    /**
     * Get the HBase table name the row belongs to
     *
     * @return the table name in bytes
     */
    byte[] getTable();

    /**
     * Get the underlying list of {@code KeyValue}s.
     * 
     * @return underlying KeyValues
     */
    List<KeyValue> getKeyValues();
    
    
    /**
     * Get the HBase {@code Result} representation of this RowData.
     * 
     * @return Result representation
     */
    Result toResult();

}
