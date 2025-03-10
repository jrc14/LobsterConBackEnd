using System;
using Azure.Data.Tables;
using Azure;


namespace LobsterConBackEnd
{
    record CloudSeqNumber : ITableEntity
    {
        public CloudSeqNumber()
        {

        }

        public CloudSeqNumber(Int64 maxSeqNumber, string remoteDevice)
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

        public Int64 MaxSeqNumber { get; set; } = default!;

        public string RemoteDevice { get; set; } = default!;
    }
}