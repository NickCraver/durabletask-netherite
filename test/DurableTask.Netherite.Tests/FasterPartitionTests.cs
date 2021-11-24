﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Xunit;
    using Xunit.Abstractions;
    using static DurableTask.Netherite.Tests.SingleHostFixture;

    [Collection("NetheriteTests")]
    public class FasterPartitionTests : IDisposable
    {
        readonly TestTraceListener traceListener;
        readonly ILoggerFactory loggerFactory;
        readonly XunitLoggerProvider provider;
        ITestOutputHelper output;

        public FasterPartitionTests(ITestOutputHelper output)
        {
            this.loggerFactory = new LoggerFactory();
            this.provider = new XunitLoggerProvider();
            this.loggerFactory.AddProvider(this.provider);
            this.traceListener = new TestTraceListener();
            Trace.Listeners.Add(this.traceListener);
            this.provider.Output = output;
            this.traceListener.Output = output;
            this.output = output;
        }

        public void Dispose()
        {
            this.provider.Output = null;
            this.traceListener.Output = null;
            this.output = null;
            Trace.Listeners.Remove(this.traceListener);
        }

        /// <summary>
        /// Create a partition and then restore it.
        /// </summary>
        [Fact]
        public async Task CreateThenRestore()
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.ResolvedTransportConnectionString = "MemoryF";
            settings.PartitionCount = 1;
            settings.HubName = $"{TestConstants.TaskHubName}-{Guid.NewGuid()}";

            var orchestrationType = typeof(ScenarioTests.Orchestrations.SayHelloInline);

            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                await service.CreateAsync();
                await service.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);
                var worker = new TaskHubWorker(service);
                var client = new TaskHubClient(service);
                worker.AddTaskOrchestrations(orchestrationType);
                await worker.StartAsync();

                // do orchestration
                var instance = await client.CreateOrchestrationInstanceAsync(orchestrationType, "0", "0");
                await client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(20));

                // stop the service
                await service.StopAsync();
            }
            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                await service.CreateAsync();
                await service.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);
                var client = new TaskHubClient(service);

                var orchestrationState = await client.GetOrchestrationStateAsync("0");
                Assert.Equal(OrchestrationStatus.Completed, orchestrationState.OrchestrationStatus);

                // stop the service
                await service.StopAsync();
            }
        }

        /// <summary>
        /// Run a number of orchestrations that requires more memory than available for FASTER
        /// </summary>
        [Fact]
        public async Task LimitedMemory()
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.PartitionCount = 1;
            settings.ResolvedTransportConnectionString = "MemoryF"; // don't bother with EventHubs for this test

            // use a fresh hubname on every run
            settings.HubName = $"{TestConstants.TaskHubName}-{Guid.NewGuid()}";

            // we don't want to take checkpoints in this test, so we set the following checkpoint triggers unattainably high
            settings.MaxNumberBytesBetweenCheckpoints = 1024L * 1024 * 1024 * 1024;
            settings.MaxNumberEventsBetweenCheckpoints = 10000000000L;
            settings.IdleCheckpointFrequencyMs = (long)TimeSpan.FromDays(1).TotalMilliseconds;

            // set the memory size very small so we can force evictions
            settings.FasterTuningParameters = new Faster.BlobManager.FasterTuningParameters()
            {
                StoreLogPageSizeBits = 10,       // 1 KB
                StoreLogMemorySizeBits = 12,     // 4 KB, which means only about 166 entries fit into memory
            };

            // create a cache monitor
            var cacheDebugger = new Faster.CacheDebugger();
            var cts = new CancellationTokenSource();
            cacheDebugger.OnError += (message) => { this.output.WriteLine($"CACHEDEBUGGER: {message}"); cts.Cancel(); };
            settings.CacheDebugger = cacheDebugger;

            // we use the standard hello orchestration from the samples, which calls 5 activities in sequence
            var orchestrationType = typeof(ScenarioTests.Orchestrations.Hello5);
            var activityType = typeof(ScenarioTests.Activities.Hello);
            string InstanceId(int i) => $"Orch{i:D5}";
            int OrchestrationCount = 100; // requires 200 FASTER key-value pairs so it does not fit into memory

            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                await service.CreateAsync();
                await service.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);
                var worker = new TaskHubWorker(service);
                var client = new TaskHubClient(service);
                worker.AddTaskOrchestrations(orchestrationType);
                worker.AddTaskActivities(activityType);
                await worker.StartAsync();

                try
                {
                    // start all orchestrations
                    {
                        var tasks = new Task[OrchestrationCount];
                        for (int i = 0; i < OrchestrationCount; i++)
                            tasks[i] = client.CreateOrchestrationInstanceAsync(orchestrationType, InstanceId(i), null);

                        var terminationTask = Task.Delay(TimeSpan.FromMinutes(5), cts.Token);
                        var completionTask = Task.WhenAll(tasks);
                        var firstTask = await Task.WhenAny(terminationTask, completionTask);
                        Assert.Same(completionTask, firstTask);
                    }

                    // wait for all orchestrations
                    {
                        async Task WaitFor(int i)
                        {
                            try
                            {
                                await client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = InstanceId(i) }, TimeSpan.FromMinutes(10));
                            }
                            catch (Exception e)
                            {
                                this.output.WriteLine($"Orchestration {InstanceId(i)} failed with {e.GetType()}: {e.Message}");
                            }
                        }

                        var tasks = new Task[OrchestrationCount];
                        for (int i = 0; i < OrchestrationCount; i++)
                            tasks[i] = WaitFor(i);

                        var terminationTask = Task.Delay(TimeSpan.FromMinutes(5), cts.Token);
                        var completionTask = Task.WhenAll(tasks);
                        var firstTask = await Task.WhenAny(terminationTask, completionTask);
                        Assert.Same(completionTask, firstTask);
                    }
                }
                finally
                {
                    foreach (var line in cacheDebugger.Dump())
                    {
                        this.output.WriteLine(line);
                    }

                    // stop the service
                    await service.StopAsync();
                }
            }
        }

        /// <summary>
        /// Create a partition and then restore it.
        /// </summary>
        [Fact]
        public async Task Locality2()
        {
            var settings = TestConstants.GetNetheriteOrchestrationServiceSettings();
            settings.ResolvedTransportConnectionString = "MemoryF";
            settings.PartitionCount = 1;

            // don't take any extra checkpoints
            settings.MaxNumberBytesBetweenCheckpoints = 1024L * 1024 * 1024 * 1024;
            settings.MaxNumberEventsBetweenCheckpoints = 10000000000L;
            settings.IdleCheckpointFrequencyMs = (long)TimeSpan.FromDays(1).TotalMilliseconds;

            //settings.HubName = $"{TestConstants.TaskHubName}-{Guid.NewGuid()}";
            settings.HubName = $"{TestConstants.TaskHubName}-Locality";

            var orchestrationType = typeof(ScenarioTests.Orchestrations.Hello5);
            var activityType = typeof(ScenarioTests.Activities.Hello);
            string InstanceId(int i) => $"Orch{i:D5}";
            int OrchestrationCount = 1000;

            {
                // start the service 
                var service = new NetheriteOrchestrationService(settings, this.loggerFactory);
                await service.CreateAsync();
                await service.StartAsync();
                var host = (TransportAbstraction.IHost)service;
                Assert.Equal(1u, service.NumberPartitions);
                var client = new TaskHubClient(service);

                // wait for all orchestrations
                {
                    var tasks = new Task[OrchestrationCount];
                    for (int i = 0; i < OrchestrationCount; i++)
                        tasks[i] = client.WaitForOrchestrationAsync(new OrchestrationInstance { InstanceId = InstanceId(i) }, TimeSpan.FromMinutes(10));
                    await Task.WhenAll(tasks);
                }

                // stop the service
                await service.StopAsync();
            }
        }
    }

}