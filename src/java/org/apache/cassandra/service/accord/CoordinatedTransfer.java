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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.CheckReturnValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.primitives.Txn;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamException;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.service.accord.CoordinatedTransfer.SingleTransferResult.State.STREAM_FAILED;

/**
 * In order to preserve data consistency for Accord, SSTable imports go through a different
 * path. Bulk transfer will place all SSTables that we want to transfer at one of the nodes. Next,
 * that node will become a coordinator and will be responsible for streaming these SSTables to the
 * corresponding replicas. Each one of these SSTables will be placed in a pending directory and
 * include metadata of the topology used. Once we receive ack's back from all nodes that they have
 * received the SSTables, we then execute a txn similar to a range txn. If the txn's executeAt does
 * correspond to the metadata in the pending directory then we move all the SSTables in
 * the pending directory to the active directory. Otherwise, the execution of the transaction is a
 * No Op.
 *
 */
public class CoordinatedTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatedTransfer.class);

    String logPrefix()
    {
        return "[CoordinatedTransfer]";
    }

    private final String keyspace;
    private final TableMetadata metadata;
    private final long streamingEpoch;
    private final TokenRange allSSTableRanges;

    final Map<Node.Id, SSTableAssignmentsPerNode> sstableAssignmentsPerNode;
    final ConcurrentMap<Node.Id, SingleTransferResult> streamResults;

    public CoordinatedTransfer(String keyspace, TableMetadata metadata, Map<Node.Id, SSTableAssignmentsPerNode> sstablesForNodes, long streamingEpoch, TokenRange allSSTableRanges)
    {
        this.keyspace = keyspace;
        this.metadata = metadata;
        this.sstableAssignmentsPerNode = sstablesForNodes;
        this.streamingEpoch = streamingEpoch;
        this.allSSTableRanges = allSSTableRanges;

        this.streamResults = new ConcurrentHashMap<>(sstablesForNodes.size());
        for (Node.Id nodeId : sstablesForNodes.keySet())
        {
            this.streamResults.put(nodeId, SingleTransferResult.Init());
        }
    }

    void execute()
    {
        logger.debug("{} Executing Accord bulk transfer {}", logPrefix(), this);
        LocalTransfers.instance().save(this);
        stream();

        if (sufficient())
        {
            // Chore: We have some issues with exhaustion? Also for reads maybe not all nodes execute them?
            TimeUUID[] planIds = streamResults.values().stream().map(result -> result.planId).toArray(TimeUUID[]::new);
            TxnRead txnRead = TxnRead.createRangeReadForSSTableImport(allSSTableRanges, planIds, streamingEpoch);
            TableMetadatas tables = TableMetadatas.of(metadata);
            TableMetadatasAndKeys tablesAndKeys = new TableMetadatasAndKeys(tables, txnRead.keys());
            Txn txn = new Txn.InMemory(Txn.Kind.Read, txnRead.keys(), txnRead, TxnQuery.NONE, null, tablesAndKeys);
            IAccordService accordService = AccordService.instance();
            accordService.coordinateAsync(Epoch.EMPTY.getEpoch(), txn, ConsistencyLevel.ALL, new Dispatcher.RequestTime(Clock.Global.nanoTime())).awaitAndGet();
            // Chore: We want the result of this coordinate async s.t. we can return whether or not this SSTable import will return an error
        }
    }

    private void stream()
    {
        List<Future<Void>> streaming = new ArrayList<>(streamResults.size());
        for (Node.Id to : streamResults.keySet())
        {
            Future<Void> stream = LocalTransfers.instance().executor.submit(() -> {
                stream(to);
                return null;
            });
            streaming.add(stream);
        }

        // Wait for all streams to complete, so we can clean up after failures. If we exit at the first failure, a
        // future stream can complete.
        LinkedList<Throwable> failures = null;
        for (Future<Void> stream : streaming)
        {
            try
            {
                stream.get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                if (failures == null)
                    failures = new LinkedList<>();
                failures.add(e);
                logger.error("{} Failed transfer due to", logPrefix(), e);
            }
        }

        /*if (failures != null && !failures.isEmpty())
        {
            Throwable failure = failures.element();
            Throwable cause = failure instanceof ExecutionException ? failure.getCause() : failure;
            maybeCleanupFailedStreams(cause);

            String msg = String.format("Failed streaming on %s instance(s): %s", failures.size(), failures);
            throw new RuntimeException(msg, Throwables.unchecked(cause));
        }*/

        logger.info("{} All streaming completed successfully", logPrefix());
    }

    private boolean sufficient()
    {
        AbstractReplicationStrategy ars = Keyspace.open(keyspace).getReplicationStrategy();
        int blockFor = ConsistencyLevel.ALL.blockFor(ars);
        int responses = 0;
        for (Map.Entry<Node.Id, SingleTransferResult> entry : streamResults.entrySet())
        {
            if (entry.getValue().state == SingleTransferResult.State.STREAM_COMPLETE)
                responses++;
        }
        return responses >= blockFor;
    }

    void stream(Node.Id to)
    {
        SingleTransferResult result;
        try
        {
            result = streamTask(to);
        }
        catch (StreamException | ExecutionException | InterruptedException | TimeoutException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            markStreamFailure(to, cause);
            throw Throwables.unchecked(cause);
        }

        try
        {
            streamComplete(to, result);
        }
        catch (ExecutionException | InterruptedException | TimeoutException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw Throwables.unchecked(cause);
        }
    }

    /*private void notifyFailure() throws ExecutionException, InterruptedException
    {
        class NotifyFailure extends AsyncFuture<Void> implements RequestCallbackWithFailure<NoPayload>
        {
            final Set<InetAddressAndPort> responses = ConcurrentHashMap.newKeySet(streamResults.size());

            @Override
            public void onResponse(Message<NoPayload> msg)
            {
                responses.remove(msg.from());
                if (responses.isEmpty())
                    trySuccess(null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                tryFailure(failure.failure);
            }
        }

        NotifyFailure notifyFailure = new NotifyFailure();
        for (Map.Entry<Node.Id, SingleTransferResult> entry : streamResults.entrySet())
        {
            InetAddressAndPort to = entry.getKey();
            // Coordinator cleans up CoordinatedTransfer and PendingLocalTransfer separately, does not need to notify
            if (FBUtilities.getBroadcastAddressAndPort().equals(to))
                continue;

            SingleTransferResult result = entry.getValue();
            if (result.planId == null)
            {
                logger.warn("{} Skipping notification of transfer failure to {} due to unknown planId", logPrefix(), to);
                continue;
            }

            logger.debug("{}, Notifying {} of transfer failure for plan {}", logPrefix(), to, result.planId);
            notifyFailure.responses.add(to);
            // Message<TransferFailed> msg = Message.out(Verb.TRANSFER_FAILED_REQ, new TransferFailed(result.planId));
            // MessagingService.instance().sendWithCallback(msg, to, notifyFailure);
        }
        notifyFailure.get();
    }*/

    private void markStreamFailure(Node.Id to, Throwable cause)
    {
        TimeUUID planId;
        if (cause instanceof StreamException)
            planId = ((StreamException) cause).finalState.planId;
        else
            planId = null;
        streamResults.computeIfPresent(to, (peer, result) -> result.streamFailed(planId));
    }

    /**
     * This shouldn't throw an exception, even if we fail to notify peers of the streaming failure.
     */
    /*private void maybeCleanupFailedStreams(Throwable cause)
    {
        try
        {
            // boolean purgeable = LocalTransfers.instance().purger.test(this);
            // if (!purgeable)
            // return;

            notifyFailure();
            // LocalTransfers.instance().scheduleCleanup();
        }
        catch (Throwable t)
        {
            if (cause != null)
                t.addSuppressed(cause);
            logger.error("{} Failed to notify peers of stream failure", logPrefix(), t);
        }
    }*/

    private void streamComplete(Node.Id to, SingleTransferResult result) throws ExecutionException, InterruptedException, TimeoutException
    {
        streamResults.put(to, result);
        logger.info("{} Completed streaming to {}, {}", logPrefix(), to, this);
    }

    /*synchronized void maybeActivate()
    {
        // If any activations have already been sent out, send new activations to any received plans that have not yet
        // been activated
        boolean anyActivated = false;
        Set<InetAddressAndPort> awaitingActivation = new HashSet<>();
        for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streamResults.entrySet())
        {
            InetAddressAndPort peer = entry.getKey();
            SingleTransferResult result = entry.getValue();
            if (result.state == COMMITTING || result.state == COMMITTED)
            {
                anyActivated = true;
            }
            else if (result.state == STREAM_COMPLETE)
                awaitingActivation.add(peer);
        }
        if (anyActivated && !awaitingActivation.isEmpty())
        {
            logger.debug("{} Transfer already activated on some peers, sending activations to remaining: {}", logPrefix(), awaitingActivation);
            activateOn(awaitingActivation);
            return;
        }
        // If no activations have been sent out, check whether we have enough planIds back to meet the required CL
        else if (sufficient())
        {
            Set<InetAddressAndPort> peers = new HashSet<>();
            for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streamResults.entrySet())
            {
                InetAddressAndPort peer = entry.getKey();
                SingleTransferResult result = entry.getValue();
                if (result.state == STREAM_COMPLETE)
                    peers.add(peer);
            }
            logger.debug("{} Transfer meets consistency level {}, sending activations to {}", logPrefix(), cl, peers);
            activateOn(peers);
            return;
        }

        logger.debug("{} Nothing to activate", logPrefix());
    }*/

    /*void activateOn(Collection<InetAddressAndPort> peers)
    {
        Preconditions.checkState(!peers.isEmpty());
        logger.debug("{} Activating {} on {}", logPrefix(), this, peers);
        LocalTransfers.instance().activating(this);

        // First phase ensures data is present on disk, then second phase does the actual import. This ensures that if
        // something goes wrong (like a topology change during import), we don't have divergence.
        class Prepare extends AsyncFuture<Void> implements RequestCallbackWithFailure<NoPayload>
        {
            final Set<InetAddressAndPort> responses = ConcurrentHashMap.newKeySet();

            public Prepare()
            {
                responses.addAll(peers);
            }

            @Override
            public void onResponse(Message<NoPayload> msg)
            {
                logger.debug("{} Got response from: {}", logPrefix(), msg.from());
                responses.remove(msg.from());
                if (responses.isEmpty())
                    trySuccess(null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                logger.debug("{} Got failure {} from {}", logPrefix(), failure, from);
                CoordinatedTransfer.this.streamResults.computeIfPresent(from, (peer, result) -> result.prepareFailed());
                tryFailure(new RuntimeException("Tracked import failed during PREPARE on " + from + " due to " + failure.reason));
            }
        }

        Prepare prepare = new Prepare();
        for (InetAddressAndPort peer : peers)
        {
            TransferActivation activation = new TransferActivation(this, peer, Phase.PREPARE);
            Message<TransferActivation> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);
            logger.debug("{} Sending {} to peer {}", logPrefix(), activation, peer);
            MessagingService.instance().sendWithCallback(msg, peer, prepare);
            CoordinatedTransfer.this.streamResults.computeIfPresent(peer, (peer0, result) -> result.preparing());
        }
        try
        {
            prepare.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw Throwables.unchecked(cause);
        }
        logger.debug("{} Activation prepare complete for {}", logPrefix(), peers);

        // Acknowledgement of activation is equivalent to a remote write acknowledgement. The imported SSTables
        // are now part of the live set, visible to reads.
        class Commit extends AsyncFuture<Void> implements RequestCallbackWithFailure<Void>
        {
            final Set<InetAddressAndPort> responses = ConcurrentHashMap.newKeySet();

            private Commit(Collection<InetAddressAndPort> peers)
            {
                responses.addAll(peers);
            }

            @Override
            public void onResponse(Message<Void> msg)
            {
                logger.debug("{} Activation successfully applied on {}", logPrefix(), msg.from());
                CoordinatedTransfer.this.streamResults.computeIfPresent(msg.from(), (peer, result) -> result.committed());

                MutationTrackingService.instance.receivedActivationResponse(CoordinatedTransfer.this, msg.from());
                responses.remove(msg.from());
                if (responses.isEmpty())
                {
                    // All activations complete, schedule cleanup to purge pending SSTables
                    LocalTransfers.instance().scheduleCleanup();
                    trySuccess(null);
                }
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                logger.error("{} Failed activation on {} due to {}", logPrefix(), from, failure);
                MutationTrackingService.instance.retryFailedTransfer(CoordinatedTransfer.this, from, failure.failure);
                // TODO(expected): should only fail if we don't meet requested CL
                tryFailure(new RuntimeException("Tracked import failed during COMMIT on " + from + " due to " + failure.reason));
            }
        }

        Commit commit = new Commit(peers);
        for (InetAddressAndPort peer : peers)
        {
            TransferActivation activation = new TransferActivation(this, peer, Phase.COMMIT);
            Message<TransferActivation> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);

            logger.debug("{} Sending {} to peer {}", logPrefix(), activation, peer);
            MessagingService.instance().sendWithCallback(msg, peer, commit);
            CoordinatedTransfer.this.streamResults.computeIfPresent(peer, (peer0, result) -> result.committing());
        }

        try
        {
            commit.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw Throwables.unchecked(cause);
        }
        logger.debug("{} Activation commit complete for {}", logPrefix(), peers);
    }*/

    public static class SSTableAssignmentsPerNode
    {
        final Node.Id id;
        final Collection<SSTableReader> sstables;
        final List<Range<Token>> ranges;

        public SSTableAssignmentsPerNode(Node.Id id, Collection<SSTableReader> sstables, List<Range<Token>> ranges)
        {
            this.id = id;
            this.sstables = sstables;
            this.ranges = ranges;
        }
    }

    /**
     * Tracks the lifecycle of a transfer from the coordinator to a single replica:
     *
     * <ul>
     *   <li>{@link State#INIT}: Transfer created, not yet streaming.</li>
     *   <li>{@link State#STREAM_COMPLETE}: Streaming successful, SSTables received on replica in pending directory.</li>
     *   <li>{@link State#STREAM_NOOP}: No data streamed (e.g., SSTable contains no rows in target range).</li>
     *   <li>{@link State#STREAM_FAILED}: Streaming failed, may not have a streaming plan ID yet.</li>
     * </ul>
     *
     * <h3>Valid State Transitions:</h3>
     * <pre>
     *
     *
     *   INIT ──┬──→ STREAM_COMPLETE
     *          │
     *          ├──→ STREAM_NOOP
     *          │
     *          └──→ STREAM_FAILED
     * </pre>
     * <p>
     * Failure states may be non-terminal if sufficient replicas reach successful states, depending on the transfer's
     * consistency level.
     */
    static class SingleTransferResult
    {
        enum State
        {
            INIT,
            STREAM_NOOP,
            STREAM_FAILED,
            STREAM_COMPLETE;

            EnumSet<State> transitionFrom;

            static
            {
                INIT.transitionFrom = EnumSet.noneOf(State.class);
                STREAM_NOOP.transitionFrom = EnumSet.of(INIT);
                STREAM_FAILED.transitionFrom = EnumSet.of(INIT);
                STREAM_COMPLETE.transitionFrom = EnumSet.of(INIT);
            }
        }

        final State state;
        private final TimeUUID planId;

        @VisibleForTesting
        SingleTransferResult(State state, TimeUUID planId)
        {
            this.state = state;
            this.planId = planId;
        }

        private boolean canTransition(SingleTransferResult.State to)
        {
            return to.transitionFrom.contains(state);
        }

        public static SingleTransferResult Init()
        {
            return new SingleTransferResult(State.INIT, null);
        }

        @VisibleForTesting
        static SingleTransferResult StreamComplete(TimeUUID planId)
        {
            return new SingleTransferResult(State.STREAM_COMPLETE, planId);
        }

        @VisibleForTesting
        static SingleTransferResult Noop()
        {
            return new SingleTransferResult(State.STREAM_NOOP, null);
        }

        @CheckReturnValue // Chore: This looks a bit wrong why is it flagging this
        private SingleTransferResult transition(State to, TimeUUID planId)
        {
            if (!canTransition(to))
            {
                logger.error("Ignoring invalid transition from {} to {}", state, to);
                return this;
            }
            return new SingleTransferResult(to, planId == null ? this.planId : planId);
        }

        @CheckReturnValue
        public SingleTransferResult streamFailed(TimeUUID planId)
        {
            return transition(STREAM_FAILED, planId);
        }

        public TimeUUID planId()
        {
            return planId;
        }

        @Override
        public String toString()
        {
            return "SingleTransferResult{" +
                   "state=" + state +
                   ", planId=" + planId +
                   '}';
        }
    }

    private SingleTransferResult streamTask(Node.Id to) throws RuntimeException, StreamException, ExecutionException, InterruptedException, TimeoutException
    {
        StreamPlan plan = new StreamPlan(StreamOperation.ACCORD_SSTABLE_IMPORT);

        // No need to flush, only using non-live SSTables already on disk
        plan.flushBeforeTransfer(false);

        SSTableAssignmentsPerNode coordinatedTransferNodeContext = sstableAssignmentsPerNode.get(to);

        for (SSTableReader sstable : coordinatedTransferNodeContext.sstables)
        {
            List<SSTableReader.PartitionPositionBounds> positions = sstable.getPositionsForRanges(coordinatedTransferNodeContext.ranges);
            long estimatedKeys = sstable.estimatedKeysForRanges(coordinatedTransferNodeContext.ranges);
            OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.ACCORD_SSTABLE_IMPORT, sstable.ref(), positions, coordinatedTransferNodeContext.ranges, estimatedKeys);
            InetAddressAndPort addr = AccordService.instance().endpointMapper().mappedEndpointOrNull(to);
            if (addr != null)
                plan.transferStreams(addr, Collections.singleton(stream));
            else
                throw new RuntimeException("IP Address for " + to.toString() + " does not exist"); // Chore: Change this into it's own exception
        }

        long timeout = DatabaseDescriptor.getStreamTransferTaskTimeout().toMilliseconds();

        logger.info("{} Starting streaming transfer {} to peer {}", logPrefix(), this, to);
        StreamResultFuture execute = plan.execute();
        StreamState state;
        try
        {
            state = execute.get(timeout, TimeUnit.MILLISECONDS);
            logger.debug("{} Completed streaming transfer {} to peer {}", logPrefix(), this, to);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            logger.error("Stream session failed with error", e);
            throw e;
        }

        if (state.hasFailedSession() || state.hasAbortedSession())
            throw new StreamException(state, "Stream failed due to failed or aborted sessions");

        // If the SSTable doesn't contain any rows in the provided range, no streams delivered, nothing to activate
        if (state.sessions().isEmpty())
            return SingleTransferResult.Noop();

        return SingleTransferResult.StreamComplete(plan.planId());
    }

    /*@Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        CoordinatedTransfer transfer = (CoordinatedTransfer) o;
        return Objects.equals(keyspace, transfer.keyspace) && Objects.equals(range, transfer.range) && Objects.equals(streamResults, transfer.streamResults) && Objects.equals(sstables, transfer.sstables) && Objects.equals(id, transfer.id);
    }*/

    /*@Override
    public int hashCode()
    {
        return Objects.hash(keyspace, range, streamResults, sstables, id);
    }*/

    /*@Override
    public String toString()
    {
        return "CoordinatedTransfer{" +
               ", keyspace='" + keyspace + '\'' +
               ", sstables=" + sstables +
               ", streamResults=" + streamResults +
               '}';
    }*/
}
