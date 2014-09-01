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

/**
 * Handles various lifecycle events by the IndexerMaster. So these will not be called on every indexer node.
 */
public interface IndexerLifecycleListener {
    /**
     * Handler for delete events
     * @param indexerDefinition Definition of indexer in question
     */
    void onDelete(IndexerDefinition indexerDefinition);

    /**
     * Handler for subscribe events. This is when the indexer is set to pick up SEP events
     * @param indexerDefinition Definition of indexer in question
     */
    void onSubscribe(IndexerDefinition indexerDefinition);

    /**
     * Handler for unsubscribe events. When the indexer stops listening for SEP events
     * @param indexerDefinition Definition of indexer in question
     */
    void onUnsubscribe(IndexerDefinition indexerDefinition);

    /**
     * Handler for started batch builds.
     * @param indexerDefinition Definition of indexer in question
     */
    void onBatchBuild(IndexerDefinition indexerDefinition);
}
