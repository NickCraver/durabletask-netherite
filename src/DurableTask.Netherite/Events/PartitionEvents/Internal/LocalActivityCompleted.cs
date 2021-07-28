﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using DurableTask.Core;

    [DataContract]
    class LocalActivityCompleted : PartitionUpdateEvent
    {
        [DataMember]
        public long ActivityId { get; set; }

        [DataMember]
        public TaskMessage Response { get; set; }

        [DataMember]
        public DateTime Timestamp { get; set; }

        [DataMember]
        public uint OriginPartitionId { get; set; }

        [DataMember]
        public int ReportedLoad { get; set; }

        [IgnoreDataMember]
        public override EventId EventId => EventId.MakePartitionInternalEventId(this.WorkItemId);

        [IgnoreDataMember]
        public string WorkItemId => ActivitiesState.GetWorkItemId(this.PartitionId, this.ActivityId);

        [IgnoreDataMember]
        public override IEnumerable<(TaskMessage,string)> TracedTaskMessages
        {
            get
            {
                yield return (this.Response, this.WorkItemId);
            }
        }

        public override void DetermineEffects(EffectTracker effects)
        {
            effects.Add(TrackedObjectKey.Activities);
        }
    }
}
