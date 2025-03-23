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
using System.Runtime.Intrinsics.X86;


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
                string purgeUser = req.Query["purgeUser"];
                string nonce = req.Query["nonce"];
                string signature = req.Query["signature"];

                string toCheck = GetHashCodeForString(syncFrom + remoteDevice + purgeUser + nonce).ToString("X8");
                if(signature!=toCheck)
                {
                    log.LogInformation("JournalSync function is rejecting a request: signature = " + signature + ";  should be " + toCheck);
                    return new UnauthorizedObjectResult("signature is incorrect");
                }


                if (string.IsNullOrEmpty(purgeUser) && !string.IsNullOrEmpty(syncFrom) && !string.IsNullOrEmpty(remoteDevice)) // a regular sync request
                {
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
                else if (!string.IsNullOrEmpty(purgeUser) && string.IsNullOrEmpty(syncFrom) && string.IsNullOrEmpty(remoteDevice))// purge user data requested
                {
                    log.LogInformation("JournalSync function is processing a request: purgeUser = " + purgeUser);

                    TableServiceClient serviceClient = new TableServiceClient(ConnectionString);
                    serviceClient.CreateTableIfNotExists("journal");
                    TableClient tcJournal = serviceClient.GetTableClient("journal");

                    string responseMessage = PurgeUserData(purgeUser, tcJournal, log);

                    log.LogInformation("JournalSync function has finished processing: purgeUser = " + purgeUser);
                    return new OkObjectResult(responseMessage);
                }
                else
                {
                    log.LogError("JournalSync rejected a request having bad parameters");
                    return new BadRequestResult();
                }
            }
            catch(Exception ex)
            {
                log.LogError(ex, "JournalSync threw an exception");

                return new NotFoundResult();
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

        // The 'purge' action will:
        //  - Amend signup create/delete actions having that user id as first half of id (replacing user id with "#deleted"),
        //  - Amend signup create/delete actions having MODIFIEDBY= that user (replacing user id with "#deleted")
        //  - Amend person create/update actions, replacing ID with "#deleted" and removing all parameters
        //  - Replace create/update sessions with the same, but with PROPOSER person handle replaced by "#deleted".
        public static string PurgeUserData(string personHandle, TableClient tcJournal, ILogger log)
        {
            try
            {
                List<JournalEntry> fetched = new List<JournalEntry>();

                Pageable<JournalEntry> queryResultsMaxPerPage = tcJournal.Query<JournalEntry>();

                foreach (Page<JournalEntry> page in queryResultsMaxPerPage.AsPages())
                {
                    foreach (JournalEntry e in page.Values)
                    {
                        fetched.Add(e);
                    }
                }

                log.LogInformation("Fetched " + fetched.Count.ToString() + " journal entr(y/ies) to be scrubbed of personal data for "+personHandle);

                string returnValue = "";

                int changes = 0;

                foreach (JournalEntry e in fetched)
                {
                    try
                    {
                        if (e.EntityType == "Person")
                        {
                            if (e.EntityId == personHandle)
                            {
                                e.EntityId = "#deleted";
                                e.Parameters = "";
                                Response response = tcJournal.UpdateEntity(e, ETag.All, TableUpdateMode.Merge);
                                changes++;
                                if(response.IsError)
                                {
                                    log.LogInformation("PurgeUserData: Error response '" + response.ReasonPhrase +"' scrubbing personal data for " + personHandle+" from "+e.RowKey);
                                }
                            }
                        }
                        else if (e.EntityType == "Session")
                        {
                            if(e.Parameters.Contains("PROPOSER|"+personHandle))
                            {
                                e.Parameters = e.Parameters.Replace("PROPOSER|" + personHandle, "PROPOSER|#deleted");
                                Response response = tcJournal.UpdateEntity(e, ETag.All, TableUpdateMode.Merge);
                                changes++;
                                if (response.IsError)
                                {
                                    log.LogInformation("PurgeUserData: Error response '" + response.ReasonPhrase + "' scrubbing personal data for " + personHandle + " from " + e.RowKey);
                                }
                            }
                        }
                        else if (e.EntityType == "SignUp")
                        {
                            bool changed = false;
                            if(e.EntityId.StartsWith(personHandle+","))
                            {
                                string sessionId = e.EntityId.Split(',')[1];
                                e.EntityId = "#deleted," + sessionId;
                                changed = true;
                            }

                            if(e.Parameters.Contains("MODIFIEDBY|"+personHandle))
                            {
                                e.Parameters = e.Parameters.Replace("MODIFIEDBY|" + personHandle, "MODIFIEDBY|#deleted");
                                changed = true;
                            }

                            if(changed)
                            {
                                Response response = tcJournal.UpdateEntity(e, ETag.All, TableUpdateMode.Merge);
                                changes++;
                                if (response.IsError)
                                {
                                    log.LogInformation("PurgeUserData: Error response '" + response.ReasonPhrase + "' scrubbing personal data for " + personHandle + " from " + e.RowKey);
                                }
                            }
                        }
                        else
                        {
                            // do nothing
                            
                        }
                        returnValue += e.ToString() + "\n"; 
                    }
                    catch (Exception ex)
                    {
                        log.LogInformation("PurgeUserData: Exception '" + ex.Message + "' scrubbing personal data for " + personHandle + " from " + e.RowKey);
                    }
                }

                log.LogInformation("Changed " + changes.ToString() + " journal entr(y/ies) to remove personal data for " + personHandle);

                return returnValue;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Failed to purge cloud entries for "+personHandle);
                return "";
            }
        }

        /// <summary>
        /// Generate a hash code for strings that is consistent between instances and launches of the app (because lately String.GetHashCode() does not seem 
        /// to be doing that for me.
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static Int32 GetHashCodeForString(string s)
        {
            try
            {
                // 
                int h = 0;
                if (!string.IsNullOrEmpty(s))
                {
                    for (int i = 0; i < s.Length; i++)
                    {
                        h = 31 * h + s[i];
                    }
                }
                return h;
            }
            catch (Exception e)
            {
                return 0;
            }
        }

        public static string ConnectionString = "DefaultEndpointsProtocol=https;AccountName=lobsterconresourceg9a08;AccountKey=G7wtsdCRCY1GFxvBd9/VQSdGTBunKe/U41MG+bG1BEcwTErLIfzRNIsW+uYzTh+EvsCDp11cE7z6+ASt3fEV9g==;EndpointSuffix=core.windows.net";

    }
}
