
/*
   Copyright (C) 2025 Turnipsoft Ltd, Jim Chapman

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/
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