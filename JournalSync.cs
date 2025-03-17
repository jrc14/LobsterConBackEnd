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
            try
            {
                string syncFrom = req.Query["syncFrom"];
                string remoteDevice = req.Query["remoteDevice"];

                log.LogInformation("JournalSync function is processing a request: syncFrom = " + syncFrom + ";  remoteDevice = " + remoteDevice);

                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

                TableServiceClient serviceClient = new TableServiceClient(ConnectionString);
                serviceClient.CreateTableIfNotExists("journal");
                serviceClient.CreateTableIfNotExists("cloudseqnumber");

                TableClient tcJournal = serviceClient.GetTableClient("journal");
                TableClient tcCloudSeqNumber = serviceClient.GetTableClient("cloudseqnumber");

                // Construct a response string consisting of all the journal entries having seq number after syncFrom
                string responseMessage = FetchEntriesSince(syncFrom, remoteDevice, tcJournal, log);

                // If we've been asked to add some new journal entries to the journal table, then add them
                if (!string.IsNullOrEmpty(requestBody.Trim()))
                {
                    // The method returns the entries (in a string, separated by \n) with updated cloud sequence numbers
                    string updatedRemoteEntries = AddNewRemoteEntries(requestBody, remoteDevice, tcJournal, tcCloudSeqNumber, log);
                    if (!string.IsNullOrEmpty(updatedRemoteEntries))
                    {
                        if (!string.IsNullOrEmpty(responseMessage))
                            responseMessage += '\n';

                        responseMessage += updatedRemoteEntries;
                    }
                }
                log.LogInformation("JournalSync function has finished processing: syncFrom = " + syncFrom + ";  remoteDevice = " + remoteDevice);

                return new OkObjectResult(responseMessage);
            }
            catch(Exception ex)
            {
                log.LogError(ex, "JournalSync threw an exception");

                return new OkObjectResult("");
            }
        }

        public static string FetchEntriesSince(string syncFrom, string remoteDevice, TableClient tcJournal, ILogger log)
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

                log.LogInformation("Fetched "+ fetched.Count.ToString() +" cloud entry/ies greater than " + syncFrom + " to be sent to " + remoteDevice);

                if (fetched.Count == 0)
                    return "";
                else if (fetched.Count == 1)
                    return fetched[0];
                else
                    return string.Join('\n', fetched);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to fetch cloud entries greater than " + syncFrom + " to be sent to " + remoteDevice);
                return "";
            }
        }

        public static string AddNewRemoteEntries(string remoteEntries, string remoteDevice, TableClient tcJournal, TableClient tcCloudSeqNumber, ILogger log)
        {
            try
            {
                // First, obtain a list of all entries that we already received from this remote device
                List<JournalEntry> existing = new List<JournalEntry>();
                try
                {
                    Pageable<JournalEntry> queryResultsMaxPerPage = tcJournal.Query<JournalEntry>(e => e.RemoteDevice==remoteDevice);

                    foreach (Page<JournalEntry> page in queryResultsMaxPerPage.AsPages())
                    {
                        foreach (JournalEntry e in page.Values)
                        {
                            existing.Add(e);
                        }
                    }

                    log.LogInformation("There are " + existing.Count.ToString() + " existing cloud entry/ies for " + remoteDevice);

                }
                catch (Exception ex)
                {
                    log.LogError(ex, "Failed to fetch existing cloud entries for " + remoteDevice);
                    return "";
                }

                List<JournalEntry> toProcess = new List<JournalEntry>(); // list of all the journal entries passed in this request from the remote device

                List<string> returnValue = new List<string>(); // list of all the journal entries whose cloud seq numbers we want the remote device to update in its journal

                List<JournalEntry> toAdd = new List<JournalEntry>(); // list of the journal entries that we want to generate new cloud seq numbers for


                if (string.IsNullOrEmpty(remoteEntries))
                {
                    log.LogInformation("Processing no remote entries from " + remoteDevice);
                }
                else if (!remoteEntries.Contains('\n'))
                {
                    JournalEntry e = JournalEntry.FromString(remoteEntries);

                    if (e == null)
                    {
                        log.LogError("remote entry is invalid: " + remoteEntries);
                        log.LogInformation("Processing no remote entries from " + remoteDevice);
                    }
                    else
                    {
                        toProcess.Add(e);
                        log.LogInformation("Processing 1 remote entry from " + remoteDevice);
                    }
                }
                else
                {
                    string[] rr = remoteEntries.Split('\n');

                    foreach (string s in rr)
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
                            toProcess.Add(e);
                        }
                    }
                    log.LogInformation("Processing " + toProcess.Count.ToString() + " remote entries from " + remoteDevice);
                }

                // Check for any cloud entries that this remote devie has already told the cloud database abourt (this is an error case that could happen if the
                // remote device sent a batch of changes to the cloud service, but disconnected or exited before writing them back to its local journal)
                foreach (JournalEntry r in toProcess)
                {
                    // if the list of existing entries for this remote device contains an entry having the same remote seq number as r, then r is a duplicate - which must not be added again
                    if (existing.Any(ee => ee.RemoteSeq == r.RemoteSeq))
                    {
                        JournalEntry duplicate = existing.Find(ee => ee.RemoteSeq == r.RemoteSeq);
                        log.LogInformation("remote entry is a duplicate: " + r.ToString()+" and won't be assigned a new cloud seq number");
                        if (!string.IsNullOrEmpty(r.RowKey))
                        {
                            log.LogError("the remote entry has a cloud seq number already - this is an error because remote devices shouldn't be asking the service to assign cloud seq numbers in this case");
                        }
                        else
                        {
                            returnValue.Add(duplicate.ToString()); // tell the remote device about the duplicate and its existing cloud seq number
                        }
                    }
                    else
                    {
                        toAdd.Add(r);
                    }
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


                    Int32 firstSeqNumberClaimed = 0;
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
                                return "";
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

                    log.LogInformation("Cloud seq number(s) [" + firstSeqNumberClaimed.ToString("X8")+ "..."+ (firstSeqNumberClaimed+ toAdd.Count-1).ToString("X8") + "] have been assigned for remote entries from " + remoteDevice);

                    // Now put the right cloud sequence numbers onto each journal entry that we're going to add, and then insert it into the table.  Collect the list of entry strings that we will
                    // return, so we can tell the caller what updated cloud sequence numbers to write back to its database
                    Int32 seq = firstSeqNumberClaimed;
                    
                    foreach (JournalEntry e in toAdd)
                    {
                        string rowKey= seq.ToString("X8");
                        try
                        {
                            e.RowKey = rowKey;
                            tcJournal.AddEntity<JournalEntry>(e);
                            returnValue.Add(e.ToString());
                            seq++;
                        }
                        catch(Exception ex)
                        {
                            log.LogError(ex, "Failed to add remote entry #" + rowKey);
                            throw;
                        }
                    }
                    log.LogInformation("Added " + toAdd.Count.ToString() + " journal entry/ies from device " + remoteDevice);
                }
                else // no entries were added to the cloud store
                {
                    log.LogInformation("Added no journal entries from device" + remoteDevice);
                }

                log.LogInformation("Returning " + returnValue.Count.ToString() + " journal entry/ies to device " + remoteDevice+ " with cloud seq numbers to be applied to the remote journal");

                if (returnValue.Count == 0)
                    return "";
                else if (returnValue.Count == 1)
                    return returnValue[0];
                else
                    return string.Join('\n', returnValue);
            }
            catch(Exception ex)
            {
                log.LogError(ex, "Failed to process remote entries for " + remoteDevice);
                return "";
            }
        }

        public static string ConnectionString = "DefaultEndpointsProtocol=https;AccountName=lobsterconresourceg9a08;AccountKey=G7wtsdCRCY1GFxvBd9/VQSdGTBunKe/U41MG+bG1BEcwTErLIfzRNIsW+uYzTh+EvsCDp11cE7z6+ASt3fEV9g==;EndpointSuffix=core.windows.net";

    }
}
