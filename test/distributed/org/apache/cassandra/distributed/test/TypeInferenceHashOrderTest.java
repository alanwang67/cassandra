/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.distributed.test;

import java.io.IOException;

import com.google.common.hash.Hashing;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.distributed.Cluster;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

/**
 * {@link AbstractType} doesn't override {@code hashCode}, so each JVM iterates hash collections of types, or of
 * functions, in its own order. Type inference depends on that order in two places:
 * <ul>
 *     <li>{@code FunctionResolver} picks the first exactly matching overload of the {@code HashMultimap} of
 *     {@code NativeFunctions}, and nested operations like {@code 1 + 1 + 2} have several.</li>
 *     <li>{@code Lists#getPreferredCompatibleType} picks the first type of a {@code HashSet} that all the elements of a
 *     collection literal are assignable to, and {@code bigint} and {@code timestamp} are assignable to each other.</li>
 * </ul>
 * In each test, every node hashes the types with its own seed, as every JVM has its own identity hash codes, and both
 * nodes run the same statements locally.
 * <p>
 */
public class TypeInferenceHashOrderTest extends TestBaseImpl
{
    @Test
    public void testAddition() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(TypeInferenceHashOrderTest::installTypeHashCodes)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v blob)"));

            // _add(X, int) is an exact match for X in tinyint, smallint, int, bigint, float, double and decimal, and a
            // blob can receive the result of any of them
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (0, 1 + 1 + 2)"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (0, 1 + 1 + 2)"));

            // to_json shows the blob in hex
            Object[][] node1 = cluster.get(1).executeInternal(withKeyspace("SELECT to_json(v) FROM %s.tbl WHERE k = 0"));
            Object[][] node2 = cluster.get(2).executeInternal(withKeyspace("SELECT to_json(v) FROM %s.tbl WHERE k = 0"));
            assertRows(node2, node1);
        }
    }

    @Test
    public void testDivision() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(TypeInferenceHashOrderTest::installTypeHashCodes)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v blob)"));

            // _divide(int, X) is an exact match for the same X, and 10 / 4 is 2 or 2.5 depending on X
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (0, (int) ? / (2 + 2))"), 10);
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (0, (int) ? / (2 + 2))"), 10);

            // to_json shows the blob in hex
            Object[][] node1 = cluster.get(1).executeInternal(withKeyspace("SELECT to_json(v) FROM %s.tbl WHERE k = 0"));
            Object[][] node2 = cluster.get(2).executeInternal(withKeyspace("SELECT to_json(v) FROM %s.tbl WHERE k = 0"));
            assertRows(node2, node1);
        }
    }

    @Test
    public void testAdditionToInt() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(TypeInferenceHashOrderTest::installTypeHashCodes)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)"));

            // _add(X, int) is an exact match for X in tinyint, smallint and int, and 100 + 100 overflows a tinyint
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (0, 100 + 100 + 2)"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (0, 100 + 100 + 2)"));

            Object[][] node1 = cluster.get(1).executeInternal(withKeyspace("SELECT v FROM %s.tbl WHERE k = 0"));
            Object[][] node2 = cluster.get(2).executeInternal(withKeyspace("SELECT v FROM %s.tbl WHERE k = 0"));
            assertRows(node2, node1);
        }
    }

    @Test
    public void testDivisionToInt() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(TypeInferenceHashOrderTest::installTypeHashCodes)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)"));

            // _divide(int, X) is an exact match for X in tinyint, smallint and int, and 100 + 100 overflows a tinyint
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (0, (int) ? / (100 + 100))"), 400);
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (0, (int) ? / (100 + 100))"), 400);

            Object[][] node1 = cluster.get(1).executeInternal(withKeyspace("SELECT v FROM %s.tbl WHERE k = 0"));
            Object[][] node2 = cluster.get(2).executeInternal(withKeyspace("SELECT v FROM %s.tbl WHERE k = 0"));
            assertRows(node2, node1);
        }
    }

    @Test
    public void testCollectionLiteral() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(TypeInferenceHashOrderTest::installTypeHashCodes)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY)"));
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (k) VALUES (0)"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (k) VALUES (0)"));

            // The literal is either a list<bigint> or a list<timestamp>, so collection_max returns a bigint or a timestamp,
            // which to_json shows as 2 or as a quoted date
            Object[][] node1 = cluster.get(1).executeInternal(withKeyspace("SELECT to_json(collection_max([(bigint) 1, (timestamp) 2])) FROM %s.tbl WHERE k = 0"));
            Object[][] node2 = cluster.get(2).executeInternal(withKeyspace("SELECT to_json(collection_max([(bigint) 1, (timestamp) 2])) FROM %s.tbl WHERE k = 0"));
            assertRows(node2, node1);
        }
    }

    /**
     * Makes the node hash the types with its own seed, standing in for the identity hash codes that are different on
     * each JVM. The seeds, 1 and 6, are picked so that the nodes disagree in all the tests.
     */
    private static void installTypeHashCodes(ClassLoader cl, int node)
    {
        new ByteBuddy().rebase(AbstractType.class)
                       .method(named("hashCode").and(takesArguments(0)))
                       .intercept(InvocationHandlerAdapter.of((type, method, args) -> Hashing.murmur3_32_fixed(node == 1 ? 1 : 6).hashUnencodedChars(type.getClass().getName()).asInt()))
                       .make()
                       .load(cl, ClassLoadingStrategy.Default.INJECTION);
    }
}
