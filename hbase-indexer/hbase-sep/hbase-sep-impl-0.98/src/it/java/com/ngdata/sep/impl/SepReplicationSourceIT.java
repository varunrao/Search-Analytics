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
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ngdata.sep.WALEditFilter;
import com.ngdata.sep.WALEditFilterProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SepReplicationSourceIT {

    private static Configuration clusterConf;
    private static HBaseTestingUtility hbaseTestUtil;

    private SepReplicationSource sepReplicationSource;


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        clusterConf = HBaseConfiguration.create();
        clusterConf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
        clusterConf.setLong("replication.source.sleepforretries", 50);
        clusterConf.set("replication.replicationsource.implementation", SepReplicationSource.class.getName());
        clusterConf.setInt("hbase.master.info.port", -1);
        clusterConf.setInt("hbase.regionserver.info.port", -1);

        hbaseTestUtil = new HBaseTestingUtility(clusterConf);

        hbaseTestUtil.startMiniZKCluster(1);
        hbaseTestUtil.startMiniCluster(1);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        hbaseTestUtil.shutdownMiniCluster();
    }

    @Before
    public void setUp() throws IOException {
        sepReplicationSource = new SepReplicationSource();
        Configuration conf = hbaseTestUtil.getConfiguration();
        ReplicationPeers replicationPeers = mock(ReplicationPeers.class);
        when(replicationPeers.getTableCFs(anyString())).thenReturn(null);
        sepReplicationSource.init(conf, FileSystem.get(conf), mock(ReplicationSourceManager.class),
                                  mock(ReplicationQueues.class), replicationPeers,
                                  mock(Stoppable.class), "/hbase.replication", UUID.randomUUID());
    }

    @Test
    public void testLoadEditFilter_CustomFilterAvailable() {
        final String subscriptionId = "_subscription_id_";

        WALEditFilter editFilter = mock(WALEditFilter.class);
        WALEditFilterProvider filterProvider = mock(WALEditFilterProvider.class);
        when(filterProvider.getWALEditFilter(subscriptionId)).thenReturn(editFilter);

        WALEditFilter loadedFilter = sepReplicationSource.loadEditFilter(subscriptionId,
                Lists.newArrayList(filterProvider));

        assertEquals(editFilter, loadedFilter);
    }

    @Test
    public void testLoadEditFilter_NoApplicableFilterAvailable() {
        final String subscriptionId = "_subscription_id_";

        WALEditFilterProvider editFilterProvider = mock(WALEditFilterProvider.class);
        when(editFilterProvider.getWALEditFilter(subscriptionId)).thenReturn(null);

        WALEditFilter loadedFilter = sepReplicationSource.loadEditFilter(subscriptionId,
                Lists.newArrayList(editFilterProvider));

        assertNull(loadedFilter);
    }

    @Test
    public void testRemoveNonReplicableEdits_CustomFilter() {
        WALEditFilter editFilter = mock(WALEditFilter.class);
        sepReplicationSource.setWALEditFilter(editFilter);

        HLog.Entry entry = buildLogEntry();

        sepReplicationSource.removeNonReplicableEdits(entry);

        verify(editFilter).apply(entry);
    }

    private HLog.Entry buildLogEntry() {
        KeyValue keyValue = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"),
                                         Bytes.toBytes("qual"), Bytes.toBytes("value"));
        HLogKey logKey = new HLogKey(Bytes.toBytes("regionName"), TableName.valueOf("table"),
                                     1L, 0L, UUID.randomUUID());
        WALEdit walEdit = new WALEdit();
        walEdit.add(keyValue);
        NavigableMap<byte[], Integer> scopes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        scopes.put(Bytes.toBytes("cf"), 1);
        logKey.setScopes(scopes);
        return new HLog.Entry(logKey, walEdit);
    }

    @Test
    public void testRemoveNonReplicableEdits_NoCustomFilter() {
        sepReplicationSource.setWALEditFilter(null);

        HLog.Entry entry = buildLogEntry();
        sepReplicationSource.removeNonReplicableEdits(entry);

        assertEquals(1, entry.getEdit().size());
    }

}
