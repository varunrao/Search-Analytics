/*
 * Copyright 2012 NGDATA nv
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

public class SepModelImplTest {
    
    @Test(expected=IllegalStateException.class)
    public void testInstantiate_InvalidZkQuorumString() {
        Configuration conf = HBaseConfiguration.create();
        
        conf.set("hbase.zookeeper.quorum", "host:2181");

        new SepModelImpl(mock(ZooKeeperItf.class), conf);
    }
    
    @Test(expected=IllegalStateException.class)
    public void testInstantiate_NonNumericZkClientPort() {
        Configuration conf = HBaseConfiguration.create();
        
        conf.set("hbase.zookeeper.property.clientPort", "not a number");
        
        new SepModelImpl(mock(ZooKeeperItf.class), conf);
    }
    

    @Test
    public void testToInternalSubscriptionName_NoSpecialCharacters() {
        assertEquals("subscription_name", SepModelImpl.toInternalSubscriptionName("subscription_name"));
    }
    
    @Test
    public void testToInternalSubscriptionName_HyphenMapping() {
        assertEquals("subscription" + SepModelImpl.INTERNAL_HYPHEN_REPLACEMENT + "name", SepModelImpl.toInternalSubscriptionName("subscription-name"));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testToInternalSubscriptionName_NameContainsMappedHyphen() {
        // We can't allow the internal hyphen replacement to ever be present on an external name,
        // otherwise we could get duplicate mapped names
        SepModelImpl.toInternalSubscriptionName("subscription" + SepModelImpl.INTERNAL_HYPHEN_REPLACEMENT + "name");
    }

}
