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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Random;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HashSharderTest {

    /*
     * this is just here to check that the default sharding strategy doesn't change
     */
    @Test
    public void testBasics() throws IOException, NoSuchAlgorithmException, SharderException {
        HashSharder sharder = new HashSharder(3);

        assertEquals(1, sharder.getShard(String.valueOf("alpha")));
        assertEquals(2, sharder.getShard(String.valueOf("beta")));
        assertEquals(1, sharder.getShard(String.valueOf("gamma")));
        assertEquals(2, sharder.getShard(String.valueOf("delta")));
    }

    /**
     * Tests many values to detect problems with modulo calculation on negative values
     * @throws IOException
     */
    @Test
    public void testIndexOutOfBounds() throws IOException, NoSuchAlgorithmException, SharderException {
        HashSharder sharder = new HashSharder(3);

        Random rg = new Random();
        for (int i = 0; i < 100; i++) {
            int shard = sharder.getShard("foo" + rg.nextInt());
            assertTrue("shard should be between 0 (inclusive) and 3 (exclusive)", shard >= 0 && shard < 3);
        }
    }

}
