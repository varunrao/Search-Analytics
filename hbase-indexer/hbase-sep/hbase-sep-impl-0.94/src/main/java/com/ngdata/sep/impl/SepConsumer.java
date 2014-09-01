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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.util.concurrent.WaitPolicy;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

/**
 * SepConsumer consumes the events for a certain SEP subscription and dispatches
 * them to an EventListener (optionally multi-threaded). Multiple SepConsumer's
 * can be started that take events from the same subscription: each consumer
 * will receive a subset of the events.
 *
 * <p>On a more technical level, SepConsumer is the remote process (the "fake
 * hbase regionserver") to which the regionservers in the hbase master cluster
 * connect to replicate log entries.</p>
 */
public class SepConsumer extends BaseHRegionServer {
    private final String subscriptionId;
    private long subscriptionTimestamp;
    private EventListener listener;
    private final ZooKeeperItf zk;
    private final Configuration hbaseConf;
    private RpcServer rpcServer;
    private ServerName serverName;
    private ZooKeeperWatcher zkWatcher;
    private SepMetrics sepMetrics;
    private final PayloadExtractor payloadExtractor;
    private String zkNodePath;
    private List<ThreadPoolExecutor> executors;
    boolean running = false;
    private Log log = LogFactory.getLog(getClass());

    /**
     * @param subscriptionTimestamp timestamp of when the index subscription became active (or more accurately, not
     *        inactive)
     * @param listener listeners that will process the events
     * @param threadCnt number of worker threads that will handle incoming SEP events
     * @param hostName hostname, this is published in ZooKeeper and will be used by HBase to connect to this
     *                 consumer, so there should be only one SepConsumer instance for a (subscriptionId, host)
     *                 combination, and the hostname should definitely not be "localhost". This is also the hostname
     *                 that the RPC will bind to.
     */
    public SepConsumer(String subscriptionId, long subscriptionTimestamp, EventListener listener, int threadCnt,
            String hostName, ZooKeeperItf zk, Configuration hbaseConf) throws IOException {
        this(subscriptionId, subscriptionTimestamp, listener, threadCnt, hostName, zk, hbaseConf, null);
    }

    /**
     * @param subscriptionTimestamp timestamp of when the index subscription became active (or more accurately, not
     *        inactive)
     * @param listener listeners that will process the events
     * @param threadCnt number of worker threads that will handle incoming SEP events
     * @param hostName hostname to bind to
     * @param payloadExtractor extracts payloads to include in SepEvents
     */
    public SepConsumer(String subscriptionId, long subscriptionTimestamp, EventListener listener, int threadCnt,
            String hostName, ZooKeeperItf zk, Configuration hbaseConf, PayloadExtractor payloadExtractor) throws IOException {
        Preconditions.checkArgument(threadCnt > 0, "Thread count must be > 0");
        this.subscriptionId = SepModelImpl.toInternalSubscriptionName(subscriptionId);
        this.subscriptionTimestamp = subscriptionTimestamp;
        this.listener = listener;
        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.sepMetrics = new SepMetrics(subscriptionId);
        this.payloadExtractor = payloadExtractor;
        this.executors = Lists.newArrayListWithCapacity(threadCnt);
        
        // TODO see same call in HBase's HRegionServer:
        // - should we do HBaseRPCErrorHandler ?
        rpcServer = HBaseRPC.getServer(this, new Class<?>[] { HRegionInterface.class }, hostName, 0, /* ephemeral port */
                10, // TODO how many handlers do we need? make it configurable?
                10, false, // TODO make verbose flag configurable
                hbaseConf, 0); // TODO need to check what this parameter is for
        
        this.serverName = new ServerName(hostName, rpcServer.getListenerAddress().getPort(), System.currentTimeMillis());
        this.zkWatcher = new ZooKeeperWatcher(hbaseConf, this.serverName.toString(), null);

        // login the zookeeper client principal (if using security)
        ZKUtil.loginClient(hbaseConf, "hbase.zookeeper.client.keytab.file",
                           "hbase.zookeeper.client.kerberos.principal", hostName);

        // login the server principal (if using secure Hadoop)
        User.login(hbaseConf, "hbase.regionserver.keytab.file",
                   "hbase.regionserver.kerberos.principal", hostName);

        for (int i = 0; i < threadCnt; i++) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(100));
            executor.setRejectedExecutionHandler(new WaitPolicy());
            executors.add(executor);
        }
    }

    public void start() throws IOException, InterruptedException, KeeperException {

        rpcServer.start();

        // Publish our existence in ZooKeeper
        zkNodePath = hbaseConf.get(SepModel.ZK_ROOT_NODE_CONF_KEY, SepModel.DEFAULT_ZK_ROOT_NODE)
                + "/" + subscriptionId + "/rs/" + serverName.getServerName();
        zk.create(zkNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        this.running = true;
    }

    public void stop() {
        Closer.close(zkWatcher);
        if (running) {
            running = false;
            Closer.close(rpcServer);
            try {
                // This ZK node will likely already be gone if the index has been removed
                // from ZK, but we'll try to remove it here to be sure
                zk.delete(zkNodePath, -1);
            } catch (Exception e) {
                log.debug("Exception while removing zookeeper node", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        sepMetrics.shutdown();
        for (ThreadPoolExecutor executor : executors) {
            executor.shutdown();
        }
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void replicateLogEntries(HLog.Entry[] entries) throws IOException {

        // TODO Recording of last processed timestamp won't work if two batches of log entries are sent out of order
        long lastProcessedTimestamp = -1;

        SepEventExecutor eventExecutor = new SepEventExecutor(listener, executors, 100, sepMetrics);
        
        for (final HLog.Entry entry : entries) {
            final HLogKey entryKey = entry.getKey();
            if (entryKey.getWriteTime() < subscriptionTimestamp) {
                continue;
            }
            byte[] tableName = entryKey.getTablename();
            Multimap<ByteBuffer, KeyValue> keyValuesPerRowKey = ArrayListMultimap.create();
            final Map<ByteBuffer, byte[]> payloadPerRowKey = Maps.newHashMap();
            for (final KeyValue kv : entry.getEdit().getKeyValues()) {
                ByteBuffer rowKey = ByteBuffer.wrap(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
                byte[] payload;
                if (payloadExtractor != null && (payload = payloadExtractor.extractPayload(tableName, kv)) != null) {
                    if (payloadPerRowKey.containsKey(rowKey)) {
                        log.error("Multiple payloads encountered for row " + Bytes.toStringBinary(kv.getRow())
                                + ", choosing " + Bytes.toStringBinary(payloadPerRowKey.get(rowKey)));
                    } else {
                        payloadPerRowKey.put(rowKey, payload);
                    }
                }
                keyValuesPerRowKey.put(rowKey, kv);
            }
            
            for (final ByteBuffer rowKeyBuffer : keyValuesPerRowKey.keySet()) {
                final List<KeyValue> keyValues = (List<KeyValue>)keyValuesPerRowKey.get(rowKeyBuffer);

                final SepEvent sepEvent = new SepEvent(tableName, keyValues.get(0).getRow(), keyValues,
                        payloadPerRowKey.get(rowKeyBuffer));
                eventExecutor.scheduleSepEvent(sepEvent);
                lastProcessedTimestamp = Math.max(lastProcessedTimestamp, entry.getKey().getWriteTime());
            }

        }
        List<Future<?>> futures = eventExecutor.flush();
        waitOnSepEventCompletion(futures);

        if (lastProcessedTimestamp > 0) {
            sepMetrics.reportSepTimestamp(lastProcessedTimestamp);
        }
    }

    private void waitOnSepEventCompletion(List<Future<?>> futures) throws IOException {
        // We should wait for all operations to finish before returning, because otherwise HBase might
        // deliver a next batch from the same HLog to a different server. This becomes even more important
        // if an exception has been thrown in the batch, as waiting for all futures increases the back-off that
        // occurs before the next attempt
        List<Exception> exceptionsThrown = Lists.newArrayList();
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted in processing events.", e);
            } catch (Exception e) {
                // While we throw the error higher up, to HBase, where it will also be logged, apparently the
                // nested exceptions don't survive somewhere, therefore log it client-side as well.
                log.warn("Error processing a batch of SEP events, the error will be forwarded to HBase for retry", e);
                exceptionsThrown.add(e);
            }
        }

        if (!exceptionsThrown.isEmpty()) {
            log.error("Encountered exceptions on " + exceptionsThrown.size() + " batches (out of " + futures.size()
                    + " total batches)");
            throw new RuntimeException(exceptionsThrown.get(0));
        }
    }
    
    @Override
    public Configuration getConfiguration() {
        return hbaseConf;
    }
    
    @Override
    public ServerName getServerName() {
        return serverName;
    }
    
    @Override
    public ZooKeeperWatcher getZooKeeper() {
        return zkWatcher;
    }

}
