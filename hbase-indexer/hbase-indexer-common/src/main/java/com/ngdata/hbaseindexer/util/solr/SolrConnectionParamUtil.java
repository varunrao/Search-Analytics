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
package com.ngdata.hbaseindexer.util.solr;

import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ngdata.hbaseindexer.SolrConnectionParams;

public class SolrConnectionParamUtil {

    private SolrConnectionParamUtil() {
        // prevent construction, utility class
    }

    public static List<String> getShards(Map<String, String> connectionParams) {
        Map<Integer, String> shardsByUserIndex = Maps.newTreeMap();
        for (Map.Entry<String, String> param : connectionParams.entrySet()) {
            if (param.getKey().startsWith(SolrConnectionParams.SOLR_SHARD_PREFIX)) {
                Integer index = Integer.valueOf(param.getKey().substring(SolrConnectionParams.SOLR_SHARD_PREFIX.length()));
                shardsByUserIndex.put(index, param.getValue());
            }
        }

        return Lists.newArrayList(shardsByUserIndex.values());
    }

    public static String getSolrMode(Map<String, String> connectionParameters) {
        return Optional.fromNullable(connectionParameters.get(SolrConnectionParams.MODE)).or("cloud").toLowerCase();
    }

    public static int getSolrMaxConnectionsPerRoute(Map<String, String> connectionParameters) {
        return Integer.parseInt(
                Optional.fromNullable(connectionParameters.get(SolrConnectionParams.MAX_CONNECTIONS_PER_HOST)).or("128"));
    }

    public static int getSolrMaxConnectionsTotal(Map<String, String> connectionParameters) {
        return Integer.parseInt(Optional.fromNullable(connectionParameters.get(SolrConnectionParams.MAX_CONNECTIONS)).or("32"));
    }

}
