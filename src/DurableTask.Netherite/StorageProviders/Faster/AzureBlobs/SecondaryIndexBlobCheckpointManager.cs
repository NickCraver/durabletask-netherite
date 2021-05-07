﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using System;
    using System.Collections.Generic;
    using FASTER.core;

    class SecondaryIndexBlobCheckpointManager : ICheckpointManager
    {
        readonly BlobManager blobManager;
        readonly int indexOrdinal;

        internal SecondaryIndexBlobCheckpointManager(BlobManager blobMan, int groupOrd)
        {
            this.blobManager = blobMan;
            this.indexOrdinal = groupOrd;
        }

        void ICheckpointManager.InitializeIndexCheckpoint(Guid indexToken)
        { } // there is no need to create empty directories in a blob container

        void ICheckpointManager.InitializeLogCheckpoint(Guid logToken)
        { } // there is no need to create empty directories in a blob container

        void ICheckpointManager.CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
            => this.blobManager.CommitIndexCheckpoint(indexToken, commitMetadata, this.indexOrdinal);

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
            => this.blobManager.CommitLogCheckpoint(logToken, commitMetadata, this.indexOrdinal);

        void ICheckpointManager.CommitLogIncrementalCheckpoint(Guid logToken, int version, byte[] commitMetadata, DeltaLog deltaLog)
        {
            // TODO: verify implementation of CommitLogIncrementalCheckpoint
            this.blobManager.CommitLogIncrementalCheckpoint(logToken, version, commitMetadata, deltaLog, this.indexOrdinal);
        }

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
            => this.blobManager.GetIndexDevice(indexToken, this.indexOrdinal);

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
            => this.blobManager.GetSnapshotLogDevice(token, this.indexOrdinal);

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
            => this.blobManager.GetSnapshotObjectLogDevice(token, this.indexOrdinal);

        IDevice ICheckpointManager.GetDeltaLogDevice(Guid token)
            => this.blobManager.GetDeltaLogDevice(token, this.indexOrdinal);

        byte[] ICheckpointManager.GetIndexCheckpointMetadata(Guid indexToken)
            => this.blobManager.GetIndexCheckpointMetadata(indexToken, this.indexOrdinal);

        byte[] ICheckpointManager.GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog)
            => this.blobManager.GetLogCheckpointMetadata(logToken, this.indexOrdinal, deltaLog);

        IEnumerable<Guid> ICheckpointManager.GetIndexCheckpointTokens()
        {
            var indexToken = this.blobManager.IndexCheckpointInfos[this.indexOrdinal].IndexToken;
            yield return indexToken;
        }

        IEnumerable<Guid> ICheckpointManager.GetLogCheckpointTokens()
        {
            var logToken = this.blobManager.IndexCheckpointInfos[this.indexOrdinal].LogToken;
            yield return logToken;
        }

        public void PurgeAll() { /* TODO */ }

        public void OnRecovery(Guid indexToken, Guid logToken)
            => this.blobManager.OnRecovery(indexToken, logToken);

        public void Dispose() { }
    }
}