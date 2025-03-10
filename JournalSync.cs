using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Data.Tables;
using Azure;
using System.Threading;
using System.Collections.Generic;
using System.Net.NetworkInformation;
using Microsoft.WindowsAzure.Storage;
using System.Net.Http.Headers;
using System.Linq;


namespace LobsterConBackEnd
{
    public static class JournalSync
    {
        [FunctionName("JournalSync")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("JournalSync function is processing a request");

            string syncFrom = req.Query["syncFrom"];
            string remoteDevice = req.Query["remoteDevice"];

            log.LogInformation("JournalSync: syncFrom = "+ syncFrom);
            log.LogInformation("JournalSync: remoteDevice = " + remoteDevice);

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            TableServiceClient serviceClient = new TableServiceClient(ConnectionString);
            serviceClient.CreateTableIfNotExists("journal");
            serviceClient.CreateTableIfNotExists("cloudseqnumber");

            TableClient tcJournal = serviceClient.GetTableClient("journal");
            TableClient tcCloudSeqNumber = serviceClient.GetTableClient("cloudseqnumber");

            // Construct a response string consisting of all the journal entries having seq number after syncFrom
            string responseMessage = FetchEntriesSince(syncFrom, tcJournal, log);

            // If we've been asked to add some new journal entries to the journal table, then add them
            if (!string.IsNullOrEmpty(requestBody.Trim()))
            {
                AddNewRemoteEntries(requestBody, remoteDevice, tcJournal, tcCloudSeqNumber, log);
            }

            return new OkObjectResult(responseMessage);
        }

        public static string FetchEntriesSince(string syncFrom, TableClient tcJournal, ILogger log)
        {
            try
            {
                List<string> fetched = new List<string>();

                Pageable<JournalEntry> queryResultsMaxPerPage = tcJournal.Query<JournalEntry>(e=>e.RowKey.CompareTo(syncFrom)>0);

                foreach (Page<JournalEntry> page in queryResultsMaxPerPage.AsPages())
                {
                    foreach (JournalEntry e in page.Values)
                    {
                        fetched.Add(e.ToString());
                    }
                }

                return string.Join('\n', fetched);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to fetch cloud entries greater than " + syncFrom);
                return "";
            }
        }

        public static void AddNewRemoteEntries(string remoteEntries, string remoteDevice, TableClient tcJournal, TableClient tcCloudSeqNumber, ILogger log)
        {
            try
            {
                List<JournalEntry> toAdd = new List<JournalEntry>();
                if (string.IsNullOrEmpty(remoteEntries))
                {
                    log.LogInformation("Adding 0 remote entries from " + remoteDevice);
                }
                else if (!remoteEntries.Contains('\n'))
                {
                    log.LogInformation("Adding 1 remote entry from " + remoteDevice);

                    JournalEntry e = JournalEntry.FromString(remoteEntries);

                    if (e == null)
                    {
                        log.LogError("remote entry is invalid: " + remoteEntries);
                    }
                    else
                    {
                        toAdd.Add(e);
                    }
                }
                else
                {
                    foreach(string s in remoteEntries.Split('\n'))
                    {
                        if (s == null || string.IsNullOrEmpty(s.Trim()))
                            continue;

                        JournalEntry e = JournalEntry.FromString(s);

                        if (e == null)
                        {
                            log.LogError("remote entry is invalid: " + s);
                        }
                        else
                        {
                            toAdd.Add(e);
                        }
                    }

                    log.LogInformation("Adding "+toAdd.Count.ToString()+" remote entries from " + remoteDevice);
                }

                if (toAdd.Count > 0)
                {
                    // first, create the cloud seq number record if it doesn't exist
                    NullableResponse<CloudSeqNumber> response1 = tcCloudSeqNumber.GetEntityIfExists<CloudSeqNumber>("1", "1");
                    if (!response1.HasValue)
                    {
                        log.LogInformation("JournalSync fuction is creating the initial CloudSeqNumber entry");

                        CloudSeqNumber seqNumberRecord = new CloudSeqNumber(0, "NONE"); // no seq number has been handed out yet; 1 will be the next number
                        tcCloudSeqNumber.AddEntity(seqNumberRecord);
                    }


                    Int64 firstSeqNumberClaimed = 0;
                    // Claim the range of cloud seq numbers that we will need to use for these added entries
                    bool clashingUpdate;
                    do
                    {
                        // Now fetch and update the record, 
                        try
                        {
                            NullableResponse<CloudSeqNumber> response = tcCloudSeqNumber.GetEntityIfExists<CloudSeqNumber>("1", "1");
                            if(!response.HasValue)
                            {
                                log.LogError("Problem with sequence number table: Failed to process remote entries for " + remoteDevice);
                                return;
                            }
                            CloudSeqNumber seqNumberRecord = response.Value;

                            firstSeqNumberClaimed = seqNumberRecord.MaxSeqNumber + 1;

                            // The N records that will be added will start at sequence number firstSeqNumberClaimed, and end at firstSeqNumberClaimed+N-1
                            // The next time that more updated entries are added, the first of those records will be given sequence number firstSeqNumberClaimed+N
                            seqNumberRecord.MaxSeqNumber = firstSeqNumberClaimed + toAdd.Count-1;

                            // If another function invocation has also claimed a seq number range while we were processing, we'll get a stroage exception now
                            tcCloudSeqNumber.UpdateEntity<CloudSeqNumber>(seqNumberRecord, seqNumberRecord.ETag, TableUpdateMode.Merge);

                            clashingUpdate = false;
                        }
                        catch (StorageException ex)
                        {
                            if (ex.RequestInformation.HttpStatusCode == 412)
                            {
                                // Optimistic concurrency violation – entity has changed since it was retrieved
                                clashingUpdate = true;
                            }
                            else
                                throw;
                        }
                    }
                    while (clashingUpdate);

                    // Now put the right cloud sequence numbers onto each journal record, and then insert it into the table
                    Int64 seq = firstSeqNumberClaimed;
                    foreach (JournalEntry e in toAdd)
                    {
                        string rowKey= seq.ToString("X8");
                        try
                        {
                            e.RowKey = rowKey;
                            tcJournal.AddEntity<JournalEntry>(e);
                            seq++;
                        }
                        catch(Exception ex)
                        {
                            log.LogError(ex, "Failed to add remote entry #" + rowKey);
                            throw;
                        }
                    }
                    
                }
            }
            catch(Exception ex)
            {
                log.LogError(ex, "Failed to add remote entries for " + remoteDevice);
            }
        }

        public static string ConnectionString = "DefaultEndpointsProtocol=https;AccountName=lobsterconresourceg9a08;AccountKey=G7wtsdCRCY1GFxvBd9/VQSdGTBunKe/U41MG+bG1BEcwTErLIfzRNIsW+uYzTh+EvsCDp11cE7z6+ASt3fEV9g==;EndpointSuffix=core.windows.net";

    }
}
