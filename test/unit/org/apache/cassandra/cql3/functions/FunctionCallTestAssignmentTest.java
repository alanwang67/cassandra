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

package org.apache.cassandra.cql3.functions;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.utils.RandomSource;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import org.apache.cassandra.cql3.AssignmentTestable.TestResult;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.ast.Expression;
import org.apache.cassandra.cql3.ast.Literal;
import org.apache.cassandra.cql3.ast.Operator;
import org.apache.cassandra.cql3.ast.TypeHint;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;

import static accord.utils.Property.qt;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.assertj.core.api.Assertions.assertThat;

public class FunctionCallTestAssignmentTest extends CQLTester
{
    private static final List<AbstractType<?>> TYPES = List.of(ByteType.instance, ShortType.instance, Int32Type.instance,
                                                               LongType.instance, IntegerType.instance, FloatType.instance,
                                                               DoubleType.instance, DecimalType.instance);

    @BeforeClass
    public static void installByteBuddy()
    {
        ByteBuddyAgent.install();
        new ByteBuddy().redefine(FunctionCall.Raw.class)
                       .visit(Advice.to(BB.class).on(named("testAssignment").and(takesArguments(String.class, ColumnSpecification.class))))
                       .make()
                       .load(FunctionCall.Raw.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());
    }

    @Test
    public void memoizationDoesNotChangeResult()
    {
        qt().check(rs -> {
            String cql = createNestedExpression(rs, rs.pick(TYPES), rs.nextInt(1, 6)).toCQL();
            for (AbstractType<?> type : TYPES)
            {
                ColumnSpecification receiver  = new ColumnSpecification(KEYSPACE, "tbl", new ColumnIdentifier("v", true), type);
                TestResult expected = withoutMemoization(() -> parse(cql).testAssignment(KEYSPACE, receiver));
                Term.Raw raw = parse(cql);
                assertThat(raw.testAssignment(KEYSPACE, receiver)).isEqualTo(expected);
            }
        });
    }

    private static Expression createNestedExpression(RandomSource rs, AbstractType<?> type, int depth)
    {
        Literal leaf = new Literal(type.fromString(String.valueOf(rs.nextInt(0, 100))), type);
        Expression expression = rs.nextBoolean() ? leaf : new TypeHint(leaf);
        if (depth == 0)
            return expression;
        return new Operator(rs.pick(Operator.Kind.values()), createNestedExpression(rs, type, depth - 1), expression);
    }

    private static Term.Raw parse(String cql)
    {
        return CQLFragmentParser.parseAny(CqlParser::term, cql, "CQL term");
    }

    private static TestResult withoutMemoization(Supplier<TestResult> fn)
    {
        BB.memoize = false;
        try
        {
            return fn.get();
        }
        finally
        {
            BB.memoize = true;
        }
    }

    public static class BB
    {
        public static volatile boolean memoize = true;

        // Clears the cache for testAssignment every time we enter the method
        @Advice.OnMethodEnter
        public static void enter(@Advice.FieldValue("cacheResults") Map<?, ?> cacheResults)
        {
            if (!memoize)
                cacheResults.clear();
        }
    }
}
