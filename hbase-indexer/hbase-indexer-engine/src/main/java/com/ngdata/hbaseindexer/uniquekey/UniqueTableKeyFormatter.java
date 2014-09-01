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
package com.ngdata.hbaseindexer.uniquekey;

import org.apache.hadoop.hbase.KeyValue;

/**
 * Format row keys into human readable form where the output contains
 * the table name of the row in question
 */
public interface UniqueTableKeyFormatter extends UniqueKeyFormatter{
    /**
     * Extracts the table name from the formatted string
     *
     * @param value Formatted value containing the table name
     * @return Representation of the table name
     */
    byte[] unformatTable (String value);

    /**
     * Format a row key into a human-readable form.
     *
     * @param row row key to be formatted
     * @param tableName
     */
    String formatRow(byte[] row, byte[] tableName);

    /**
     * Format a column family value into a human-readable form.
     * <p>
     * Called as part of column-based mapping, {@link com.ngdata.hbaseindexer.conf.IndexerConf.MappingType#COLUMN}.
     *
     * @param family family bytes to be formatted
     * @param tableName
     */
    String formatFamily(byte[] family, byte[] tableName);

    /**
     * Format a {@code KeyValue} into a human-readable form. Only the row, column family, and qualifier
     * of the {@code KeyValue} will be encoded.
     * <p>
     * Called in case of column-based mapping, {@link com.ngdata.hbaseindexer.conf.IndexerConf.MappingType#COLUMN}.
     *
     * @param keyValue value to be formatted
     * @param tableName
     */
    String formatKeyValue(KeyValue keyValue, byte[] tableName);


}
