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
package com.ngdata.hbaseindexer.model.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Information about the last run batch index build, stored as part of an {@link IndexerDefinition}.
 *
 * <p>This object is immutable. Modifying it yields a new instance.</p>
 */
public class BatchBuildInfo {
    private long submitTime;
    private Boolean finishedSuccessful;
    private Map<String, String> mapReduceJobTrackingUrls = Collections.emptyMap();
    private String[] batchIndexCliArguments;

    public BatchBuildInfo(long submitTime, Boolean finishedSuccessful, Map<String, String> mapReduceJobTrackingUrls,
                          String[] batchIndexCliArguments) {
        this.submitTime = submitTime;
        this.finishedSuccessful = finishedSuccessful;
        this.mapReduceJobTrackingUrls =
                mapReduceJobTrackingUrls != null ? mapReduceJobTrackingUrls : Collections.<String, String>emptyMap();
        this.batchIndexCliArguments = batchIndexCliArguments;
    }

    /**
     * Copy constructor.
     */
    public BatchBuildInfo(BatchBuildInfo batchBuildInfo) {
        this(batchBuildInfo.submitTime, batchBuildInfo.finishedSuccessful, batchBuildInfo.mapReduceJobTrackingUrls,
                batchBuildInfo.batchIndexCliArguments);
    }

    public BatchBuildInfo() {
        // nothing here
    }

    public long getSubmitTime() {
        return submitTime;
    }

    /**
     * @return true if finished successfully, false if finished unsuccessfully, null if not finished
     */
    public Boolean isFinishedSuccessful() {
        return finishedSuccessful;
    }

    public Map<String, String> getMapReduceJobTrackingUrls() {
        return Collections.unmodifiableMap(mapReduceJobTrackingUrls);
    }

    public BatchBuildInfo withJob(String jobId, String jobTrackingUrl) {
        Map<String, String> newMap = Maps.newHashMap(mapReduceJobTrackingUrls);
        newMap.put(jobId, jobTrackingUrl);

        BatchBuildInfo batchBuildInfo = new BatchBuildInfo(this);
        batchBuildInfo.mapReduceJobTrackingUrls = newMap;
        return batchBuildInfo;
    }

    public String[] getBatchIndexCliArguments() {
        return batchIndexCliArguments;
    }

    public BatchBuildInfo finishedSuccessfully(boolean finishedSuccessful) {
        BatchBuildInfo batchBuildInfo = new BatchBuildInfo(this);
        batchBuildInfo.finishedSuccessful = finishedSuccessful;
        return batchBuildInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BatchBuildInfo that = (BatchBuildInfo) o;

        if (submitTime != that.submitTime) return false;
        if (!Arrays.equals(batchIndexCliArguments, that.batchIndexCliArguments)) return false;
        if (finishedSuccessful != null ? !finishedSuccessful.equals(that.finishedSuccessful) : that.finishedSuccessful != null)
            return false;
        if (mapReduceJobTrackingUrls != null ? !mapReduceJobTrackingUrls.equals(that.mapReduceJobTrackingUrls) :
                that.mapReduceJobTrackingUrls != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (submitTime ^ (submitTime >>> 32));
        result = 31 * result + (finishedSuccessful != null ? finishedSuccessful.hashCode() : 0);
        result = 31 * result + (mapReduceJobTrackingUrls != null ? mapReduceJobTrackingUrls.hashCode() : 0);
        result = 31 * result + (batchIndexCliArguments != null ? Arrays.hashCode(batchIndexCliArguments) : 0);
        return result;
    }
}

