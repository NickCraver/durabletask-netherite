﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.EventHubsTransport
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using DurableTask.Netherite.Abstractions;
    using Microsoft.Azure.EventHubs;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Extensions.Logging;

    class EventHubsProcessor : IEventProcessor, TransportAbstraction.IDurabilityListener
    {
        readonly TransportAbstraction.IHost host;
        readonly TransportAbstraction.ISender sender;
        readonly TaskhubParameters parameters;
        readonly EventHubsTraceHelper traceHelper;
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly EventHubsTransport eventHubsTransport;
        readonly PartitionContext partitionContext;
        readonly string eventHubName;
        readonly string eventHubPartition;
        readonly byte[] taskHubGuid;
        readonly uint partitionId;
        readonly CancellationToken shutdownToken;
        readonly BlobBatchReceiver<PartitionEvent> blobBatchReceiver;

        //private uint partitionId;
        CancellationTokenSource eventProcessorShutdown;
        // we set this task once shutdown has been initiated
        Task shutdownTask = null;

        // we occasionally checkpoint received packets with eventhubs. It is not required for correctness
        // as we filter duplicates anyway, but it will help startup time.
        long persistedSequenceNumber;
        long persistedOffset;
        long? lastCheckpointedOffset;

        // since EventProcessorHost does not redeliver packets, we need to keep them around until we are sure
        // they are processed durably, so we can redeliver them when recycling/recovering a partition
        // we make this a concurrent queue so we can remove confirmed events concurrently with receiving new ones
        readonly ConcurrentQueue<(PartitionEvent[] events, long offset, long seqno)> pendingDelivery;
        AsyncLock deliveryLock;

        // this points to the latest incarnation of this partition; it gets
        // updated as we recycle partitions (create new incarnations after failures)
        volatile Task<PartitionIncarnation> currentIncarnation;

        /// <summary>
        /// The event processor can recover after exceptions, so we encapsulate
        /// the currently active partition
        /// </summary>
        class PartitionIncarnation
        {
            public int Incarnation;
            public IPartitionErrorHandler ErrorHandler;
            public TransportAbstraction.IPartition Partition;
            public Task<PartitionIncarnation> Next;
            public (long seqNo, int batchPos) NextPacketToReceive;
            public int SuccessiveStartupFailures;
        }

        readonly Dictionary<string, MemoryStream> reassembly = new Dictionary<string, MemoryStream>();

        public EventHubsProcessor(
            TransportAbstraction.IHost host,
            TransportAbstraction.ISender sender,
            TaskhubParameters parameters,
            PartitionContext partitionContext,
            NetheriteOrchestrationServiceSettings settings,
            EventHubsTransport eventHubsTransport,
            EventHubsTraceHelper traceHelper,
            CancellationToken shutdownToken)
        {
            this.host = host;
            this.sender = sender;
            this.parameters = parameters;
            this.pendingDelivery = new ConcurrentQueue<(PartitionEvent[] events, long offset, long seqno)>();
            this.partitionContext = partitionContext;
            this.settings = settings;
            this.eventHubsTransport = eventHubsTransport;
            this.eventHubName = this.partitionContext.EventHubPath;
            this.eventHubPartition = this.partitionContext.PartitionId;
            this.taskHubGuid = parameters.TaskhubGuid.ToByteArray();
            this.partitionId = uint.Parse(this.eventHubPartition);
            this.traceHelper = new EventHubsTraceHelper(traceHelper, this.partitionId);
            this.shutdownToken = shutdownToken;
            string traceContext = $"EventHubsProcessor {this.eventHubName}/{this.eventHubPartition}";
            this.blobBatchReceiver = new BlobBatchReceiver<PartitionEvent>(traceContext, this.traceHelper, this.settings, keepUntilConfirmed: true);

            var _ = shutdownToken.Register(
              () => { var _ = Task.Run(() => this.IdempotentShutdown("shutdownToken", eventHubsTransport.FatalExceptionObserved)); },
              useSynchronizationContext: false);
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is opening", this.eventHubName, this.eventHubPartition);
            this.eventProcessorShutdown = new CancellationTokenSource();
            this.deliveryLock = new AsyncLock();

            // make sure we shut down as soon as the partition is closing
            var _ = context.CancellationToken.Register(
              () => { var _ = Task.Run(() => this.IdempotentShutdown("context.CancellationToken", true)); },
              useSynchronizationContext: false);

            // we kick off the start-and-retry mechanism for the partition, but don't wait for it to be fully started.
            // instead, we save the task and wait for it when we need it
            this.currentIncarnation = Task.Run(() => this.StartPartitionAsync());

            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} opened", this.eventHubName, this.eventHubPartition);
            return Task.CompletedTask;
        }

        public void ConfirmDurable(Event evt)
        {
            // this is called after an event has committed (i.e. has been durably persisted in the recovery log).
            // so we know we will never need to deliver it again. We remove it from the local buffer, and also checkpoint
            // with EventHubs occasionally.
            while (this.pendingDelivery.TryPeek(out var front) && front.events[front.events.Length - 1].NextInputQueuePosition <= ((PartitionEvent)evt).NextInputQueuePosition)
            {
                if (this.pendingDelivery.TryDequeue(out var candidate))
                {
                    this.persistedOffset = Math.Max(this.persistedOffset, candidate.offset);
                    this.persistedSequenceNumber = Math.Max(this.persistedSequenceNumber, candidate.seqno);
                }
            }
        }

        async Task<PartitionIncarnation> StartPartitionAsync(PartitionIncarnation prior = null)
        {
            // create the record for this incarnation
            var c = new PartitionIncarnation()
            {
                Incarnation = (prior != null) ? (prior.Incarnation + 1) : 1,
                ErrorHandler = this.host.CreateErrorHandler(this.partitionId),
            };

            // if this is not the first incarnation, stay on standby until the previous incarnation is terminated.
            if (c.Incarnation > 1)
            {
                try
                {
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is readying next startup (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);

                    await Task.Delay(-1, prior.ErrorHandler.Token);
                }
                catch (OperationCanceledException)
                {
                }

                if (!this.eventProcessorShutdown.IsCancellationRequested)
                {
                    // we are now becoming the current incarnation
                    this.currentIncarnation = prior.Next;

                    // sometimes we can get stuck into a loop of failing attempts to reincarnate a partition. 
                    // We don't want to waste CPU  and pollute the logs, but we also can't just give up because
                    // the failures can be transient. Thus we back off the retry pace.
                    TimeSpan addedDelay = 
                          (prior.SuccessiveStartupFailures < 2)   ? TimeSpan.Zero
                        : (prior.SuccessiveStartupFailures < 10)  ? TimeSpan.FromSeconds(1)
                        : (prior.SuccessiveStartupFailures < 100) ? TimeSpan.FromSeconds(5)
                        : (prior.SuccessiveStartupFailures < 200) ? TimeSpan.FromSeconds(10)
                                                                  : TimeSpan.FromMinutes(1);


                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is restarting partition (incarnation {incarnation}) soon; addedDelay={addedDelay}", this.eventHubName, this.eventHubPartition, c.Incarnation, addedDelay);

                    if (addedDelay != TimeSpan.Zero)
                    {
                        await Task.Delay(addedDelay);
                    }

                    // we wait at most 20 seconds for the previous partition to terminate cleanly
                    int tries = 4;
                    var timeout = TimeSpan.FromSeconds(5);

                    while (!await prior.ErrorHandler.WaitForTermination(timeout))
                    {
                        this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} partition (incarnation {incarnation}) is still waiting for PartitionShutdown of previous incarnation", this.eventHubName, this.eventHubPartition, c.Incarnation);

                        if (--tries == 0)
                        {
                            break;
                        }
                    }
                }
            }

            // check that we are not already shutting down before even starting this
            if (this.eventProcessorShutdown.IsCancellationRequested)
            {
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is cancelling startup of incarnation {incarnation}", this.eventHubName, this.eventHubPartition, c.Incarnation);
                return null;
            }

            // start the next incarnation task, will be on standby until after the current one is terminated
            c.Next = this.StartPartitionAsync(c);

            try
            {
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is starting partition (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);

                // to handle shutdown before startup completes, register a force-termination
                using var registration = this.eventProcessorShutdown.Token.Register(
                    () => c.ErrorHandler.HandleError(
                        nameof(StartPartitionAsync),
                        "EventHubsProcessor shut down before partition fully started",
                        null,
                        terminatePartition: true,
                        reportAsWarning: true));

                // start this partition (which may include waiting for the lease to become available)
                c.Partition = this.host.AddPartition(this.partitionId, this.sender);
                c.NextPacketToReceive = await c.Partition.CreateOrRestoreAsync(c.ErrorHandler, this.parameters, this.eventHubsTransport.Fingerprint);

                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} started partition (incarnation {incarnation}), next expected packet is #{nextSeqno}", this.eventHubName, this.eventHubPartition, c.Incarnation, c.NextPacketToReceive);

                // receive packets already sitting in the buffer; use lock to prevent race with new packets being delivered
                using (await this.deliveryLock.LockAsync())
                {
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} checking for packets requiring redelivery (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);
                    var batch = this.pendingDelivery
                        .SelectMany(x => x.Item1)
                        .Where(evt => (evt.NextInputQueuePosition, evt.NextInputQueueBatchPosition).CompareTo(c.NextPacketToReceive) > 0)
                        .ToList();
                    if (batch.Count > 0)
                    {
                        var lastInBatch = batch[batch.Count - 1];
                        c.NextPacketToReceive = (lastInBatch.NextInputQueuePosition, lastInBatch.NextInputQueueBatchPosition);
                        c.Partition.SubmitEvents(batch);
                        this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} redelivered {batchsize} packets, starting with #{seqno}, next expected packet is #{nextSeqno} (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, batch.Count, batch[0].NextInputQueuePosition - 1, c.NextPacketToReceive, c.Incarnation);
                    }
                    else
                    {
                        this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} has no packets requiring redelivery ", this.eventHubName, this.eventHubPartition);
                    }
                }

                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} completed partition startup (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);
            }
            catch (OperationCanceledException) when (c.ErrorHandler.IsTerminated)
            {
                // the partition startup was canceled
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} canceled partition startup (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, c.Incarnation);
            }
            catch (Exception e)
            {
                c.SuccessiveStartupFailures = 1 + (prior?.SuccessiveStartupFailures ?? 0);
                c.ErrorHandler.HandleError("EventHubsProcessor.StartPartitionAsync", "failed to start partition", e, true, false);
                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} failed during startup (incarnation {incarnation}): {exception}", this.eventHubName, this.eventHubPartition, c.Incarnation, e);
            }        

            return c;
        }

        async Task IdempotentShutdown(string reason, bool quickly)
        {
            async Task ShutdownAsync()
            {
                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is shutting down (reason: {reason}, quickly: {quickly})", this.eventHubName, this.eventHubPartition, reason, quickly);

                this.eventProcessorShutdown.Cancel(); // stops reincarnations

                PartitionIncarnation current = await this.currentIncarnation;

                while (current != null && current.ErrorHandler.IsTerminated)
                {
                    current = await current.Next;
                }

                if (current == null)
                {
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} already canceled or terminated", this.eventHubName, this.eventHubPartition);
                }
                else
                {
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} stopping partition (incarnation: {incarnation}, quickly: {quickly})", this.eventHubName, this.eventHubPartition, current.Incarnation, quickly);
                    await current.Partition.StopAsync(quickly);
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} stopped partition (incarnation {incarnation})", this.eventHubName, this.eventHubPartition, current.Incarnation);
                }

                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is shut down", this.eventHubName, this.eventHubPartition);
            }

            using (await this.deliveryLock.LockAsync())
            {
                if (this.shutdownTask == null)
                {
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} starting shutdown task", this.eventHubName, this.eventHubPartition);
                    this.shutdownTask = Task.Run(() => ShutdownAsync());
                }
                else
                {
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} shutdown task already started", this.eventHubName, this.eventHubPartition);
                }
            }

            await this.shutdownTask;
        }

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is closing (reason: {reason})", this.eventHubName, this.eventHubPartition, reason);

            if (reason != CloseReason.LeaseLost)
            {
                await this.SaveEventHubsReceiverCheckpoint(context, 0);
            }

            await this.IdempotentShutdown("CloseAsync", reason == CloseReason.LeaseLost);

            this.deliveryLock.Dispose();

            this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} closed", this.eventHubName, this.eventHubPartition);
        }   

        async ValueTask SaveEventHubsReceiverCheckpoint(PartitionContext context, long byteThreshold)
        {
            if (this.lastCheckpointedOffset.HasValue
                && this.persistedOffset - this.lastCheckpointedOffset.Value > byteThreshold
                && !context.CancellationToken.IsCancellationRequested)
            {
                var checkpoint = new Checkpoint(this.partitionId.ToString(), this.persistedOffset.ToString(), this.persistedSequenceNumber);

                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition} is checkpointing receive position through #{seqno}", this.eventHubName, this.eventHubPartition, checkpoint.SequenceNumber);
                try
                {
                    await context.CheckpointAsync(checkpoint);
                    this.lastCheckpointedOffset = long.Parse(checkpoint.Offset);
                }
                catch (Exception e)
                {
                    // updating EventHubs checkpoints has been known to fail occasionally due to leases shifting around; since it is optional anyway
                    // we don't want this exception to cause havoc
                    this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} failed to checkpoint receive position: {e}", this.eventHubName, this.eventHubPartition, e);

                    if (Utils.IsFatal(e))
                    {
                        this.host.OnFatalExceptionObserved(e);
                    }
                }
            }
        }

        async Task IEventProcessor.ProcessErrorAsync(PartitionContext context, Exception exception)
        {

            LogLevel logLevel;

            switch (exception)
            {
                case ReceiverDisconnectedException: 

                    // occurs when partitions are being rebalanced by EventProcessorHost
                    logLevel = LogLevel.Information;

                    // since this processor is no longer going to receive events, let's shut it down
                    // one would expect that this is redundant with EventProcessHost calling close
                    // but empirically we have observed that the latter does not always happen in this situation
                    var _ = Task.Run(() => this.IdempotentShutdown("Receiver was disconnected", true));
                    break;

                case Microsoft.Azure.EventHubs.MessagingEntityNotFoundException:

                    // occurs when partition hubs was deleted either accidentally, or intentionally after messages were lost due to the retention limit
                    logLevel = LogLevel.Warning;
                    this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} EventHub was deleted, initiating recovery via restart", this.eventHubName, this.eventHubPartition);
                    await this.eventHubsTransport.ExitProcess(false);
                    break;
                    
                default:
                    logLevel = LogLevel.Warning;
                    break;
            }

            this.traceHelper.Log(logLevel, "EventHubsProcessor {eventHubName}/{eventHubPartition} received internal error indication from EventProcessorHost: {exception}", this.eventHubName, this.eventHubPartition, exception);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> packets)
        {
            var first = packets.FirstOrDefault();
            long firstSequenceNumber = first?.SystemProperties.SequenceNumber ?? 0;
            
            this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition} is receiving events starting with #{seqno}", this.eventHubName, this.eventHubPartition, firstSequenceNumber);

            PartitionIncarnation current = await this.currentIncarnation;

            while (current != null && current.ErrorHandler.IsTerminated)
            {
                current = await current.Next;
            }

            if (current == null)
            {
                this.traceHelper.LogWarning("EventHubsProcessor {eventHubName}/{eventHubPartition} received packets for closed processor, discarded", this.eventHubName, this.eventHubPartition);
                return;
            }
            else
            {
                this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition} is delivering to incarnation {seqno}", this.eventHubName, this.eventHubPartition, current.Incarnation);
            }

            if (!this.lastCheckpointedOffset.HasValue)
            {
                // the first packet we receive indicates what our last checkpoint was
                this.lastCheckpointedOffset = first == null ? null : long.Parse(first.SystemProperties.Offset);

                // we may be missing packets if the service was down for longer than EH retention
                if (firstSequenceNumber > current.NextPacketToReceive.seqNo)
                {
                    this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition} missing packets in sequence, #{seqno} instead of #{expected}. Initiating recovery via delete and restart.", this.eventHubName, this.eventHubPartition, firstSequenceNumber, current.NextPacketToReceive);
                    await this.eventHubsTransport.ExitProcess(true);
                }
            }

            try
            {
                var receivedTimestamp = current.Partition.CurrentTimeMs;
                int totalEvents = 0;
                Stopwatch stopwatch = Stopwatch.StartNew();

                using (await this.deliveryLock.LockAsync()) // must prevent rare race with a partition that is currently restarting. Contention is very unlikely.
                {
                    this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation}) is processing packets", this.eventHubName, this.eventHubPartition, current.Incarnation);

                    await foreach ((EventData eventData, PartitionEvent[] events, long seqNo) in this.blobBatchReceiver.ReceiveEventsAsync(this.taskHubGuid, packets, current.ErrorHandler.Token, current.NextPacketToReceive.seqNo))
                    {
                        for (int i = 0; i < events.Length; i++)
                        {
                            PartitionEvent evt = events[i];

                            if (i < events.Length - 1)
                            {
                                evt.NextInputQueuePosition = seqNo;
                                evt.NextInputQueueBatchPosition = i + 1;
                            }
                            else
                            {
                                evt.NextInputQueuePosition = seqNo + 1;
                            }

                            if (this.traceHelper.IsEnabled(LogLevel.Trace))
                            {
                                this.traceHelper.LogTrace("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation}) received packet #{seqno}.{subSeqNo} {event} id={eventId}", this.eventHubName, this.eventHubPartition, current.Incarnation, seqNo, i, evt, evt.EventIdString);
                            }

                            totalEvents++;
                        }
 
                        // add events to redelivery queue, and attach durability listener to last update event in the batch
                        this.pendingDelivery.Enqueue((events, long.Parse(eventData.SystemProperties.Offset), eventData.SystemProperties.SequenceNumber));
                        for (int i = events.Length - 1; i >= 0; i--)
                        {
                            if (events[i] is PartitionUpdateEvent partitionUpdateEvent)
                            {
                                DurabilityListeners.Register(partitionUpdateEvent, this);
                            }
                        }

                        if (current.NextPacketToReceive.batchPos == 0)
                        {
                            current.Partition.SubmitEvents(events);
                        }
                        else
                        {
                            this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation}) skipping {batchPos} events in batch #{seqno} because they are already processed", this.eventHubName, this.eventHubPartition, current.Incarnation, current.NextPacketToReceive.batchPos, seqNo);
                            current.Partition.SubmitEvents(events.Skip(current.NextPacketToReceive.batchPos).ToList());
                        }

                        current.NextPacketToReceive = (seqNo + 1, 0);
                    }
                }

                this.traceHelper.LogDebug("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation}) received {totalEvents} events in {latencyMs:F2}ms, starting with #{seqno}, next expected packet is #{nextSeqno}", this.eventHubName, this.eventHubPartition, current.Incarnation, totalEvents, stopwatch.Elapsed.TotalMilliseconds, firstSequenceNumber, current.NextPacketToReceive.seqNo);

                await this.SaveEventHubsReceiverCheckpoint(context, 600000);
            }
            catch (OperationCanceledException)
            {
                this.traceHelper.LogInformation("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation}) was terminated", this.eventHubName, this.eventHubPartition, current.Incarnation);
            }
            catch (Exception exception)
            {
                this.traceHelper.LogError("EventHubsProcessor {eventHubName}/{eventHubPartition}({incarnation}) encountered an exception while processing packets : {exception}", this.eventHubName, this.eventHubPartition, current.Incarnation, exception);
                current?.ErrorHandler.HandleError("IEventProcessor.ProcessEventsAsync", "Encountered exception while processing events", exception, true, false);
            }
        }
    }
}
