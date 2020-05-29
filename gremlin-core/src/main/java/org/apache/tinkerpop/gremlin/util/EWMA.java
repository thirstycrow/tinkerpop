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
package org.apache.tinkerpop.gremlin.util;

import java.util.concurrent.TimeUnit;

public class EWMA {
    private final double tau;
    private long stamp = System.nanoTime();
    private double ewma = 0d;

    public EWMA(final int decayTime, final TimeUnit decayUnit) {
        tau = decayUnit.toNanos(decayTime);
    }

    public synchronized void observe(final double value) {
        final long t = System.nanoTime();
        final long td = Math.max(0, t - stamp);
        final double w = Math.exp(- td / tau);
        ewma = ewma * w + value * (1 - w);
        stamp = t;
    }

    public synchronized double get() {
        observe(0d);
        return ewma;
    }
}
