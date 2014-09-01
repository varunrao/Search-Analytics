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
package com.ngdata.sep.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

/**
 * Some utility methods that can be useful when writing test cases that involve the SEP.
 *
 * This class is specifc to HBase 0.98 implementation.
 */
public class SepTestUtil extends SepTestUtilCommon {
    private static final String MBEAN_NAME = "Hadoop:service=HBase,name=RegionServer,sub=Replication";
    private static final String ATTR_NAME = "source.sizeOfLogQueue"; 

    public static void waitOnReplication(Configuration conf, long timeout) throws Exception {
        SepTestUtilCommon.waitOnReplication(conf, timeout, TableName.META_TABLE_NAME.toString(), MBEAN_NAME, ATTR_NAME); 
    }
}
