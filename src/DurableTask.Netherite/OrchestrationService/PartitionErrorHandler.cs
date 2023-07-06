// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;

    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    // For indicating and initiating termination, and for tracing errors and warnings relating to a partition.
    // Is is basically a wrapper around CancellationTokenSource with features for diagnostics.
    class PartitionErrorHandler : IPartitionErrorHandler
    {
        readonly CancellationTokenSource cts = new CancellationTokenSource();
        readonly int partitionId;
        readonly ILogger logger;
        readonly LogLevel logLevelLimit;
        readonly string account;
        readonly string taskHub;
        readonly List<Task> disposeTasks;
        readonly TaskCompletionSource<int> disposed;

        public CancellationToken Token
        {
            get
            {
                try
                {
                    return this.cts.Token;
                }
                catch (ObjectDisposedException)
                {
                    return new CancellationToken(true);
                }
            }
        }

        public bool IsTerminated => this.terminationStatus != NotTerminated;

        public bool NormalTermination =>  this.terminationStatus == TerminatedNormally;

        public Task<int> WaitForDisposeTasksAsync => this.disposed.Task;

        volatile int terminationStatus = NotTerminated;
        const int NotTerminated = 0;
        const int TerminatedWithError = 1;
        const int TerminatedNormally = 2;

        public PartitionErrorHandler(int partitionId, ILogger logger, LogLevel logLevelLimit, string storageAccountName, string taskHubName)
        {
            this.cts = new CancellationTokenSource();
            this.partitionId = partitionId;
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.disposeTasks = new List<Task>();
            this.disposed = new TaskCompletionSource<int>();
        }
     
        public void HandleError(string context, string message, Exception exception, bool terminatePartition, bool isWarning)
        {
            this.TraceError(isWarning, context, message, exception, terminatePartition);

            // terminate this partition in response to the error

            if (terminatePartition && this.terminationStatus == NotTerminated)
            {
                if (Interlocked.CompareExchange(ref this.terminationStatus, TerminatedWithError, NotTerminated) == NotTerminated)
                {
                    this.Terminate();
                }
            }
        }

        public void TerminateNormally()
        {
            if (Interlocked.CompareExchange(ref this.terminationStatus, TerminatedNormally, NotTerminated) == NotTerminated)
            {
                this.Terminate();
            }
        }

        void TraceError(bool isWarning, string context, string message, Exception exception, bool terminatePartition)
        {
            var logLevel = isWarning ? LogLevel.Warning : LogLevel.Error;
            if (this.logLevelLimit <= logLevel)
            {
                // for warnings, do not print the entire exception message
                string details = exception == null ? string.Empty : (isWarning ? $"{exception.GetType().FullName}: {exception.Message}" : exception.ToString());
                
                this.logger?.Log(logLevel, "Part{partition:D2} !!! {message} in {context}: {details} terminatePartition={terminatePartition}", this.partitionId, message, context, details, terminatePartition);

                if (isWarning)
                {
                    EtwSource.Log.PartitionWarning(this.account, this.taskHub, this.partitionId, context, terminatePartition, message, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
                else
                {
                    EtwSource.Log.PartitionError(this.account, this.taskHub, this.partitionId, context, terminatePartition, message, details, TraceUtils.AppName, TraceUtils.ExtensionVersion);
                }
            }
        }

        void Terminate()
        {
            try
            {
                // this immediately cancels all activities depending on the error handler token
                this.cts.Cancel();
            }
            catch (AggregateException aggregate)
            {
                foreach (var e in aggregate.InnerExceptions)
                {
                    this.HandleError("PartitionErrorHandler.Terminate", "Exception in PartitionCancellation", e, false, true);
                }
            }
            catch (Exception e)
            {
                this.HandleError("PartitionErrorHandler.Terminate", "Exception in PartitionCancellation", e, false, true);
            }
            finally
            {
                // now that the partition is dead, run all the dispose tasks
                Task.Run(this.DisposeAsync);
            }
        }

        public void AddDisposeTask(Action action)
        {
            this.disposeTasks.Add(new Task(action));
        }

        async Task DisposeAsync()
        {
            try
            {
                var tasks = this.disposeTasks;
                foreach (var task in tasks)
                {
                    task.Start();
                }
                await Task.WhenAll(tasks);

                // we can now dispose the cancellation token source itself
                this.cts.Dispose();

                this.disposed.SetResult(tasks.Count);
            }
            catch (Exception e)
            {
                this.disposed.SetException(e);
            }
        }
    }
}
