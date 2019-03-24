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
package org.apache.tinkerpop.machine.function.reduce;

import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.ReduceFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountReduce<C, S, E> extends AbstractFunction<C> implements ReduceFunction<C, S, Map<E, Long>> {

    private final Compilation<C, S, E> byCompilation;

    public GroupCountReduce(final Coefficient<C> coefficient, final Set<String> labels, final Compilation<C, S, E> byCompilation) {
        super(coefficient, labels);
        this.byCompilation = byCompilation;
    }

    @Override
    public Map<E, Long> apply(final Traverser<C, S> traverser, final Map<E, Long> currentValue) {
        E object = null == this.byCompilation ? (E) traverser.object() : this.byCompilation.mapObject(traverser.object()).object();
        currentValue.put(object, traverser.coefficient().count() + currentValue.getOrDefault(object, 0L));
        return currentValue;
    }

    @Override
    public Map<E, Long> merge(final Map<E, Long> valueA, final Map<E, Long> valueB) {
        final Map<E, Long> newMap = new HashMap<>();
        newMap.putAll(valueA);
        newMap.putAll(valueB);
        return newMap;
    }

    @Override
    public Map<E, Long> getInitialValue() {
        return new HashMap<>();
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.byCompilation);
    }

}