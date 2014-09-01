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

import org.apache.hadoop.hbase.HBaseTestingUtility;

/**
 * Creates HBaseTestingUtility objects that can be used in parallel (i.e.
 * they don't use any standard default HBase ports and have the WebUI
 * disabled).
 */
public class HBaseTestingUtilityFactory {
    
    public static HBaseTestingUtility createTestUtility() {
        HBaseTestingUtility testUtil = new HBaseTestingUtility();
        testUtil.getConfiguration().setInt("hbase.master.info.port", -1);
        testUtil.getConfiguration().setInt("hbase.regionserver.info.port", -1);
        return testUtil;
    }

}
