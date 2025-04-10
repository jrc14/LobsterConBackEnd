using System;
using Azure.Data.Tables;
using Azure;


namespace LobsterConBackEnd
{
    /// <summary>
    /// The single-row table that keeps track of which cloud sequence number we will issue next, when we receive new journal entries to add to the cloud store.
    /// </summary>
    record CloudSeqNumber : ITableEntity
    {
        public CloudSeqNumber()
        {

        }

        public CloudSeqNumber(Int32 maxSeqNumber, string remoteDevice)
        {
            this.PartitionKey = "1";
            this.RowKey = "1";
            this.MaxSeqNumber = maxSeqNumber;
            this.RemoteDevice = remoteDevice;
        }

        public string PartitionKey { get; set; } = default!;

        public string RowKey { get; set; } = default!;

        public ETag ETag { get; set; } = default!;

        public DateTimeOffset? Timestamp { get; set; } = default!;

        public Int32 MaxSeqNumber { get; set; } = default!;

        public string RemoteDevice { get; set; } = default!;
    }
}