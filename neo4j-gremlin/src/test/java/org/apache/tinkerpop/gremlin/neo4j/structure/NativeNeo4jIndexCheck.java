/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.neo4j.structure;

import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.neo4j.AbstractNeo4jGremlinTest;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NativeNeo4jIndexCheck extends AbstractNeo4jGremlinTest {
    private static final Logger logger = LoggerFactory.getLogger(NativeNeo4jIndexCheck.class);

    @Test
    public void shouldHaveFasterRuntimeWithLabelKeyValueIndex() throws Exception {
        final Neo4jGraph neo4j = (Neo4jGraph) this.graph;
        int maxVertices = 10000;
        for (int i = 0; i < maxVertices; i++) {
            if (i % 2 == 0)
                this.graph.addVertex(T.label, "something", "myId", i);
            else
                this.graph.addVertex(T.label, "nothing", "myId", i);
        }
        this.graph.tx().commit();

        // traversal
        final Runnable traversal = () -> {
            final Traversal<Vertex, Vertex> t = g.V().hasLabel("something").has("myId", 2000);
            final Vertex vertex = t.tryNext().get();
            assertFalse(t.hasNext());
            assertEquals(1, IteratorUtils.count(vertex.properties("myId")));
            assertEquals("something", vertex.label());
        };

        // no index
        this.graph.tx().readWrite();
        assertFalse(this.getBaseGraph().hasSchemaIndex("something", "myId"));
        TimeUtil.clock(20, traversal);
        final double noIndexTime = TimeUtil.clock(20, traversal);
        // index time
        neo4j.cypher("CREATE INDEX ON :something(myId)").iterate();
        this.graph.tx().commit();
        this.graph.tx().readWrite();
        Thread.sleep(5000); // wait for indices to be built
        assertTrue(this.getBaseGraph().hasSchemaIndex("something", "myId"));
        TimeUtil.clock(20, traversal);
        final double indexTime = TimeUtil.clock(20, traversal);
        logger.info("Query time (no-index vs. index): {}", noIndexTime + " vs. {}", indexTime);
        assertTrue((noIndexTime / 10) > indexTime); // should be at least 10x faster
    }

    @Test
    public void shouldReturnResultsLabeledIndexOnVertexWithHasHas() {
        this.graph.tx().readWrite();
        this.getBaseGraph().execute("CREATE INDEX ON :Person(name)", null);
        this.graph.tx().commit();
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.tx().commit();
        assertEquals(2, this.g.V().has(T.label, "Person").has("name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void shouldEnsureColonedKeyIsTreatedAsNormalKey() {
        this.graph.tx().readWrite();
        this.getBaseGraph().execute("CREATE INDEX ON :Person(name)", null);
        this.graph.tx().commit();
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.tx().commit();
        assertEquals(2, this.g.V().has(T.label, "Person").has("name", "marko").count().next(), 0);
        assertEquals(0, this.g.V().has("Person:name", "marko").count().next(), 0);
    }

    @Test
    public void shouldReturnResultsUsingLabeledIndexOnVertexWithHasHasHas() {
        this.graph.tx().readWrite();
        this.getBaseGraph().execute("CREATE INDEX ON :Person(name)", null);
        this.graph.tx().commit();
        this.graph.addVertex(T.label, "Person", "name", "marko", "color", "blue");
        this.graph.addVertex(T.label, "Person", "name", "marko", "color", "green");
        this.graph.tx().commit();
        assertEquals(1, this.g.V().has(T.label, "Person").has("name", "marko").has("color", "blue").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void shouldReturnResultsOnVertexWithHasHasHasNoIndex() {
        this.graph.tx().commit();
        this.graph.addVertex(T.label, "Person", "name", "marko", "color", "blue");
        this.graph.addVertex(T.label, "Person", "name", "marko", "color", "green");
        this.graph.tx().commit();
        assertEquals(1, this.g.V().has(T.label, "Person").has("name", "marko").has("color", "blue").count().next(), 0);
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void shouldReturnResultsUsingLabeledIndexOnVertexWithColonFails() {
        this.graph.tx().readWrite();
        this.getBaseGraph().execute("CREATE INDEX ON :Person(name)", null);
        this.graph.tx().commit();
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.tx().commit();
        assertNotEquals(2l, this.g.V().has("Person:name", "marko").count().next().longValue());
        assertEquals(2, this.g.V().has("name", "marko").count().next(), 0);
    }

    @Test
    public void shouldEnforceUniqueConstraint() {
        this.graph.tx().readWrite();
        this.getBaseGraph().execute("CREATE CONSTRAINT ON (p:Person) assert p.name is unique", null);
        this.graph.tx().commit();
        this.graph.tx().commit();
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.tx().commit();
        assertEquals("marko", g.V().has(T.label, "Person").has("name", "marko").next().value("name"));
    }

    @Test
    public void shouldEnforceMultipleUniqueConstraint() {
        this.graph.tx().readWrite();
        this.getBaseGraph().execute("CREATE CONSTRAINT ON (p:Person) assert p.name is unique", null);
        this.getBaseGraph().execute("CREATE CONSTRAINT ON (p:Person) assert p.surname is unique", null);
        this.graph.tx().commit();
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.addVertex(T.label, "Person", "surname", "aaaa");
        this.graph.tx().commit();
        boolean failSurname = false;
        try {
            this.graph.addVertex(T.label, "Person", "surname", "aaaa");
        } catch (RuntimeException e) {
            if (isConstraintViolation(e)) failSurname = true;
        }
        assertTrue(failSurname);
        boolean failName = false;
        try {
            this.graph.addVertex(T.label, "Person", "name", "marko");
        } catch (RuntimeException e) {
            if (isConstraintViolation(e)) failName = true;
        }
        assertTrue(failName);
        this.graph.tx().commit();
    }

    private boolean isConstraintViolation(RuntimeException e) {
        return e.getClass().getSimpleName().equals("ConstraintViolationException");
    }

    @Test
    public void shouldDropMultipleUniqueConstraint() {
        this.graph.tx().readWrite();
        this.getBaseGraph().execute("CREATE CONSTRAINT ON (p:Person) assert p.name is unique", null);
        this.getBaseGraph().execute("CREATE CONSTRAINT ON (p:Person) assert p.surname is unique", null);
        this.graph.tx().commit();

        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.addVertex(T.label, "Person", "surname", "aaaa");
        this.graph.tx().commit();
        boolean failSurname = false;
        try {
            this.graph.addVertex(T.label, "Person", "surname", "aaaa");
        } catch (RuntimeException e) {
            if (isConstraintViolation(e)) failSurname = true;
        }
        assertTrue(failSurname);
        boolean failName = false;
        try {
            this.graph.addVertex(T.label, "Person", "name", "marko");
        } catch (RuntimeException e) {
            if (isConstraintViolation(e)) failName = true;
        }
        assertTrue(failName);
        this.graph.tx().commit();

        this.graph.tx().readWrite();
        this.getBaseGraph().execute("DROP CONSTRAINT ON (p:Person) assert p.name is unique", null);
        this.getBaseGraph().execute("DROP CONSTRAINT ON (p:Person) assert p.surname is unique", null);

        this.graph.tx().commit();
        assertEquals(1, this.g.V().has(T.label, "Person").has("name", "marko").count().next(), 0);
        assertEquals(1, this.g.V().has(T.label, "Person").has("surname", "aaaa").count().next(), 0);
        this.graph.addVertex(T.label, "Person", "surname", "aaaa");
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.tx().commit();
        assertEquals(2, this.g.V().has(T.label, "Person").has("name", "marko").count().next(), 0);
        assertEquals(2, this.g.V().has(T.label, "Person").has("surname", "aaaa").count().next(), 0);
    }

    @Test(expected = RuntimeException.class)
    public void shouldFailUniqueConstraint() {
        this.graph.tx().readWrite();
        this.getBaseGraph().execute("CREATE CONSTRAINT ON (p:Person) assert p.name is unique", null);
        this.graph.tx().commit();
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.tx().commit();
        assertEquals("marko", g.V().has(T.label, "Person").has("name", "marko").next().value("name"));
        this.graph.addVertex(T.label, "Person", "name", "marko");
    }

    @Test
    public void shouldDoLabelAndIndexSearch() {
        graph.tx().readWrite();
        this.getBaseGraph().execute("CREATE INDEX ON :Person(name)", null);
        this.graph.tx().commit();

        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.addVertex(T.label, "Person", "name", "john");
        this.graph.addVertex(T.label, "Person", "name", "pete");
        this.graph.tx().commit();
        assertEquals(1, this.g.V().has("Person", "name", "marko").count().next(), 0);
        assertEquals(3, this.g.V().has(T.label, "Person").count().next(), 0);
        assertEquals(1, this.g.V().has(T.label, "Person").has("name", "marko").count().next(), 0);
    }

    @Test
    public void shouldDoLabelsNamespaceBehavior() {
        graph.tx().readWrite();

        this.getBaseGraph().execute("CREATE INDEX ON :Person(name)", null);
        this.getBaseGraph().execute("CREATE INDEX ON :Product(name)", null);
        this.getBaseGraph().execute("CREATE INDEX ON :Corporate(name)", null);

        this.graph.tx().commit();
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.addVertex(T.label, "Person", "name", "john");
        this.graph.addVertex(T.label, "Person", "name", "pete");
        this.graph.addVertex(T.label, "Product", "name", "marko");
        this.graph.addVertex(T.label, "Product", "name", "john");
        this.graph.addVertex(T.label, "Product", "name", "pete");
        this.graph.addVertex(T.label, "Corporate", "name", "marko");
        this.graph.addVertex(T.label, "Corporate", "name", "john");
        this.graph.addVertex(T.label, "Corporate", "name", "pete");
        this.graph.tx().commit();
        assertEquals(1, this.g.V().has(T.label, "Person").has("name", "marko").has(T.label, "Person").count().next(), 0);
        assertEquals(1, this.g.V().has(T.label, "Product").has("name", "marko").has(T.label, "Product").count().next(), 0);
        assertEquals(1, this.g.V().has(T.label, "Corporate").has("name", "marko").has(T.label, "Corporate").count().next(), 0);
        assertEquals(0, this.g.V().has(T.label, "Person").has("name", "marko").has(T.label, "Product").count().next(), 0);
        assertEquals(0, this.g.V().has(T.label, "Product").has("name", "marko").has(T.label, "Person").count().next(), 0);
        assertEquals(0, this.g.V().has(T.label, "Corporate").has("name", "marko").has(T.label, "Person").count().next(), 0);
    }
}
