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
package org.apache.tinkerpop.gremlin.driver;

import com.codahale.metrics.*;
import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.exception.HostUnavailableException;
import org.apache.tinkerpop.gremlin.util.MetricManager;
import org.apache.tinkerpop.gremlin.util.EWMA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class ConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    public static final int MAX_WAITERS = 100;
    public static final int MIN_POOL_SIZE = 2;
    public static final int MAX_POOL_SIZE = 8;
    public static final int LOW_WATERMARK = 4;
    public static final int HIGH_WATERMARK = 16;

    public final Host host;
    private final Cluster cluster;
    private final Client client;
    private final List<Connection> connections;
    private final Set<Connection> bin = new CopyOnWriteArraySet<>();
    private final int minPoolSize;
    private final int maxPoolSize;
    private final int lowWatermark;
    private final int highWatermark;
    private final String poolLabel;

    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

    private final Connector connector = new Connector();
    private final Lender lender = new Lender(MAX_WAITERS);
    private final Metrics metrics = new Metrics();

    public ConnectionPool(final Host host, final Client client) {
        this(host, client, Optional.empty(), Optional.empty());
    }

    public ConnectionPool(final Host host, final Client client, final Optional<Integer> overrideMinPoolSize,
                          final Optional<Integer> overrideMaxPoolSize) {
        this.host = host;
        this.client = client;
        this.cluster = client.cluster;
        poolLabel = String.format("Connection Pool {host=%s}", host);

        final Settings.ConnectionPoolSettings settings = settings();
        this.minPoolSize = overrideMinPoolSize.orElse(settings.minSize);
        this.maxPoolSize = overrideMaxPoolSize.orElse(settings.maxSize);
        this.lowWatermark = settings.lowWatermark;
        this.highWatermark = settings.highWatermark;

        this.connections = new CopyOnWriteArrayList<>();
        connector.activate();

        logger.info("Opening connection pool on {} with core size of {}", host, minPoolSize);
    }

    public Settings.ConnectionPoolSettings settings() {
        return cluster.connectionPoolSettings();
    }

    public CompletableFuture<Connection> borrowConnection(final long deadline) {
        BorrowRequest request = new BorrowRequest(deadline);
        lender.borrow(request);
        return request.future;
    }

    public void returnConnection(final Connection connection) throws ConnectionException {
        logger.debug("Attempting to return {} on {}", connection, host);
        metrics.returnedTotal.inc();
        if (isClosed()) throw new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown");

        if (connection.isDead()) {
            logger.debug("Marking {} as dead", this.host);
            connector.removeConnection(connection);
        } else if (!connector.maybeDestroyConnection(connection)) {
            lender.activate();
        }
    }

    Client getClient() {
        return client;
    }

    Cluster getCluster() {
        return cluster;
    }

    public boolean isClosed() {
        return closeFuture.get() != null;
    }

    /**
     * Permanently kills the pool.
     */
    public CompletableFuture<Void> closeAsync() {
        if (closeFuture.compareAndSet(null, new CompletableFuture<>())) {
            lender.activate();
            CompletableFuture.allOf(connections.stream()
                    .map(this::closeConnectionAsync)
                    .toArray(CompletableFuture[]::new)
            ).whenComplete((r, e) -> closeFuture.get().complete(null));
        }
        return closeFuture.get();
    }

    private CompletableFuture<Void> closeConnectionAsync(Connection connection) {
        metrics.disconnectedTotal.inc();
        return connection.closeAsync();
    }

    /**
     * Required for testing
     */
    int numConnectionsWaitingToCleanup() {
        return bin.size();
    }

    private Connection selectLeastUsed() {
        int minInFlight = Integer.MAX_VALUE;
        Connection leastBusy = null;
        for (Connection connection : connections) {
            final int inFlight = connection.borrowed.get();
            if (!connection.isDead() && inFlight < minInFlight && connection.availableInProcess() > 0) {
                minInFlight = inFlight;
                leastBusy = connection;
            }
        }
        return leastBusy;
    }

    private void makeAvailable() {
        host.makeAvailable();
        cluster.loadBalancingStrategy().onAvailable(host);
    }

    private void maybeUnavailable() {
        if (connections.size() == 0 && host.isAvailable()) {
            host.makeUnavailable();
            cluster.loadBalancingStrategy().onUnavailable(host);
            cluster.executor().schedule(
                    () -> connector.activate(),
                    cluster.connectionPoolSettings().reconnectInterval,
                    TimeUnit.MILLISECONDS);
            lender.activate();
        }
    }

    public String getPoolInfo() {
        final StringBuilder sb = new StringBuilder("ConnectionPool (");
        sb.append(host);
        sb.append(") - ");
        connections.forEach(c -> {
            sb.append(c);
            sb.append(",");
        });
        return sb.toString().trim();
    }

    @Override
    public String toString() {
        return poolLabel;
    }

    private class BorrowRequest {
        final long deadline;
        final long start = System.nanoTime();
        private final CompletableFuture<Connection> future = new CompletableFuture();

        BorrowRequest(long deadline) {
            this.deadline = deadline;
            metrics.borrowsTotal.inc();
        }

        public void complete(Connection connection) {
            if (future.complete(connection)) {
                metrics.borrowedTotal.inc();
                metrics.waitingTime.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            }
        }

        public void fail(Throwable cause) {
            if (future.completeExceptionally(cause)) {
                metrics.borrowFailuresTotal.inc();
                metrics.waitingTime.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            }
        }
    }

    private abstract class AtMostOneWorker {

        private final Lock lock = new ReentrantLock();

        public abstract boolean maybeHasWork();

        public abstract boolean doWork();

        public final void activate() {
            cluster.executor().submit(() -> {
                while (maybeHasWork() && lock.tryLock()) {
                    try {
                        if (!doWork()) {
                            break;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            });
        }
    }

    private class Connector extends AtMostOneWorker {
        private AtomicLong adjustTime = new AtomicLong(System.nanoTime());
        private int targetPoolSize = minPoolSize;
        private int open = 0;

        void removeConnection(final Connection connection) {
            if (connections.remove(connection)) {
                open--;
                bin.add(connection);
                maybeUnavailable();
                activate();
            }
            maybeDestroyConnection(connection);
        }

        boolean maybeDestroyConnection(final Connection connection) {
            if (bin.contains(connection) && (connection.isDead() || connection.borrowed.get() == 0)) {
                if (bin.remove(connection)) {
                    closeConnectionAsync(connection);
                    logger.debug("{} destroyed", connection.getConnectionInfo());
                    return true;
                }
            }
            return false;
        }

        void maybeAdjustPoolSize() {
            final long adjust = adjustTime.get();
            final long now = System.nanoTime();
            if (now > adjust && adjustTime.compareAndSet(adjust, now + TimeUnit.SECONDS.toNanos(1))) {
                final double ewma = metrics.waitingQLength.get();
                if (ewma < lowWatermark) {
                    targetPoolSize = Math.max(minPoolSize, targetPoolSize - 1);
                    activate();
                } else if (ewma > highWatermark) {
                    targetPoolSize = Math.min(maxPoolSize, targetPoolSize + 1);
                    activate();
                }
            }
        }

        @Override
        public boolean maybeHasWork() {
            return open != targetPoolSize;
        }

        @Override
        public boolean doWork() {
            if (open < targetPoolSize) {
                metrics.connectsTotal.inc();
                open++;
                Connection.open(host.getHostUri(), ConnectionPool.this, settings().maxInProcessPerConnection)
                        .whenCompleteAsync((connection, error) -> {
                            if (error == null) {
                                metrics.connectedTotal.inc();
                                connections.add(connection);
                                makeAvailable();
                                lender.activate();
                            } else {
                                metrics.connectionFailuresTotal.inc();
                                open--;
                                maybeUnavailable();
                            }
                        }, cluster.executor());
            } else {
                Connection leastUsed = selectLeastUsed();
                if (leastUsed != null) {
                    removeConnection(leastUsed);
                }
            }
            return host.isAvailable();
        }
    }

    private class Lender extends AtMostOneWorker {
        private final BlockingQueue<BorrowRequest> waiters;

        Lender(int maxWaiters) {
            waiters = new ArrayBlockingQueue<>(maxWaiters);
        }

        void borrow(BorrowRequest request) {
            if (waiters.offer(request)) {
                metrics.waitingQLength.observe(waiters.size());
                cluster.executor().submit(() -> activate());
                connector.maybeAdjustPoolSize();
            } else {
                request.fail(new HostUnavailableException(host.getHostUri(), host.getAddress(), "Host is busy"));
            }
        }

        @Override
        public boolean maybeHasWork() {
            return !waiters.isEmpty();
        }

        @Override
        public boolean doWork() {
            final BorrowRequest request = waiters.peek();
            if (System.nanoTime() > request.deadline) {
                request.fail(new ConnectionException(host.getHostUri(), host.getAddress(), "Timed out waiting for connection"));
            } else if (isClosed()) {
                request.fail(new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown"));
            } else if (!host.isAvailable()) {
                request.fail(new HostUnavailableException(host.getHostUri(), host.getAddress(), "Host is unavailable"));
            } else {
                tryBorrowConnection(request);
            }
            boolean requestDone = request.future.isDone();
            if (requestDone) {
                waiters.poll();
                metrics.waitingQLength.observe(waiters.size());
                connector.maybeAdjustPoolSize();
            }
            return requestDone;
        }

        private void tryBorrowConnection(final BorrowRequest request) {
            final Connection leastUsedConn = selectLeastUsed();

            if (null == leastUsedConn) {
                if (isClosed()) {
                    request.fail(new ConnectionException(host.getHostUri(), host.getAddress(), "Pool is shutdown"));
                    return;
                }
                logger.debug("Pool was initialized but a connection could not be selected earlier - waiting for connection on {}", host);
                return;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Return least used {} on {}", leastUsedConn.getConnectionInfo(), host);
            }
            leastUsedConn.borrowed.incrementAndGet();
            request.complete(leastUsedConn);
        }
    }

    private class Metrics {
        final Counter connectsTotal = MetricManager.INSTANCE.getCounter(name("connects_total"));
        final Counter connectedTotal = MetricManager.INSTANCE.getCounter(name("connected_total"));
        final Counter disconnectedTotal = MetricManager.INSTANCE.getCounter(name("disconnected_total"));
        final Counter connectionFailuresTotal = MetricManager.INSTANCE.getCounter(name("connection_failures_total"));
        final Counter borrowsTotal = MetricManager.INSTANCE.getCounter(name("borrows_total"));
        final Counter borrowedTotal = MetricManager.INSTANCE.getCounter(name("borrowed_total"));
        final Counter borrowFailuresTotal = MetricManager.INSTANCE.getCounter(name("borrow_failures_total"));
        final Counter returnedTotal = MetricManager.INSTANCE.getCounter(name("returned_total"));
        final Timer waitingTime = MetricManager.INSTANCE.getTimer(name("waiting_for_connection"));
        final EWMA waitingQLength = new EWMA(10, TimeUnit.SECONDS);

        Metrics() {
            MetricManager.INSTANCE.getRegistry().register(name("waiting_q_length"), (Gauge) waitingQLength::get);
        }

        private String name(String... names) {
            return MetricRegistry.name(ConnectionPool.class, names);
        }
    }
}
