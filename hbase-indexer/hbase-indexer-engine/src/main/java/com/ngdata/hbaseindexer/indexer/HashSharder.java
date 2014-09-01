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
package com.ngdata.hbaseindexer.indexer;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.google.common.base.Preconditions;

/**
 * Basic sharder based on output from md5
 */
public class HashSharder implements Sharder {

    private int numShards;
    private MessageDigest mdAlgorithm;

    public HashSharder(int numShards) throws SharderException {
        Preconditions.checkArgument(numShards > 0, "There should be at least one shard");
        this.numShards = numShards;
        try {
            this.mdAlgorithm = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new SharderException("failed to initialize MD5 digest", e);
        }
    }

    private long hash(String key) throws SharderException {
        try {
            // Cloning message digest rather than looking it up each time
            MessageDigest md = (MessageDigest) mdAlgorithm.clone();
            byte[] digest = md.digest(key.getBytes("UTF-8"));
            return ((digest[0] & 0xFF) << 8) + ((digest[1] & 0xFF));
        } catch (UnsupportedEncodingException e) {
            throw new SharderException("Error calculating hash.", e);
        } catch (CloneNotSupportedException e) {
            // Sun's MD5 supports cloning, so we don't expect this to happen
            throw new RuntimeException(e);
        }
    }

    public int getShard(String id) throws SharderException {
        long a = hash(id);
        return (int) ((a % numShards + numShards) % numShards); // make sure we get positive values
    }

}
