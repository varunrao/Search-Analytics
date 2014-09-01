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

import static org.junit.Assert.assertEquals;

import java.util.Map;

import com.ngdata.hbaseindexer.mr.HBaseIndexerMapper;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class HBaseIndexerMapperTest {

    @Test
    public void testConfigureIndexConnectionParams() {
        Map<String,String> connectionParams = ImmutableMap.of("key1", "val1", "key2", "val2");
        Configuration conf = new Configuration();
        
        HBaseIndexerMapper.configureIndexConnectionParams(conf, connectionParams);
        assertEquals(connectionParams, HBaseIndexerMapper.getIndexConnectionParams(conf));
    }
    
    @Test
    public void testGetIndexConnectionParams_Null() {
        assertEquals(
                ImmutableMap.of(),
                HBaseIndexerMapper.getIndexConnectionParams(new Configuration()));
    }

}
