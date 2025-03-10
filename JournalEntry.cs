using System;
using Azure.Data.Tables;
using Azure;
using Microsoft.AspNetCore.JsonPatch.Operations;


namespace LobsterConBackEnd
{
    record JournalEntry : ITableEntity
    {
        public JournalEntry()
        {

        }

        public JournalEntry(string cloudSeq, string remoteSeq, string remoteDevice, string eventFilter, string entityType, string operationType, string entityId, string parameters)
        {
            this.PartitionKey = eventFilter;
            this.RowKey = cloudSeq;
            this.RemoteSeq = remoteSeq;
            this.RemoteDevice = remoteDevice;
            this.EventFilter = eventFilter;
            this.EntityType = entityType;
            this.OperationType = operationType;
            this.EntityId = entityId;
            this.Parameters = Parameters;

        }

        public static JournalEntry FromString(string s)
        {
            string[] ss = s.Split('\\');

            if (ss.Length < 7)
                return null;
            else
            {
                string parameters = "";
                if (ss.Length == 8)
                {
                    parameters = ss[7];
                }
                JournalEntry e = new JournalEntry(ss[0], ss[1], ss[2], ss[3], ss[4], ss[5], ss[6], parameters);
                return e;
            }
        }

        public override string ToString()
        {
            string s =
                this.RowKey + "\\" +
                this.RemoteSeq + "\\" +
                this.RemoteDevice + "\\" +
                this.EventFilter + "\\" +
                this.EntityType + "\\" +
                this.OperationType + "\\" +
                this.EntityId;
            if(!string.IsNullOrEmpty(this.Parameters))
            {
                s += "\\" + this.Parameters;
            }
            return s;
        }

        public string PartitionKey { get; set; } = default!;

        public string RowKey { get; set; } = default!;

        public ETag ETag { get; set; } = default!;

        public DateTimeOffset? Timestamp { get; set; } = default!;

        public string RemoteSeq { get; set; } = default!;

        public string RemoteDevice { get; set; } = default!;

        public string EventFilter { get; set; } = default!;

        public string EntityType { get; set; } = default!;

        public string OperationType { get; set; } = default!;

        public string EntityId { get; set; } = default!;

        public string Parameters { get; set; } = default!;
    }
}
