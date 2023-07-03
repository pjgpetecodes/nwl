using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;

namespace nwl.uprnfinder
{
    public static class EventHubTrigger1
    {

        static HttpClient client = new HttpClient();

        [FunctionName("EventHubTrigger1")]
        public static async Task Run([EventHubTrigger("nwluprn", Connection = "nwleventhub1_RootManageSharedAccessKey_EVENTHUB")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            

            foreach (EventData eventData in events)
            {
                try
                {
                    // Replace these two lines with your processing logic.
                    //log.LogInformation($"C# Event Hub trigger function processed a message: {eventData.EventBody}");

                    // Cast the Event Body to a csvline object
                    var csvline = eventData.EventBody.ToObjectFromJson<CsvLine>();

                    // if the csvline.customerid == 10000 then log out that we've reached the end
                    if (csvline.CustomerID == "10000")
                    {
                        log.LogInformation($"C# Event Hub trigger function processed the last message: {eventData.EventBody}");

                        // Make a call to the following API endpoint: https://nwluprnapi.azurewebsites.net/api/HttpTrigger1?code=bgsYSm-_PlxnRQ0EU0YvfespzQ021leJh57vaEmf-py0AzFu4KwoSw== with the csvline object as the body
                        HttpResponseMessage response = await client.PostAsJsonAsync("https://nwluprnapi.azurewebsites.net/api/HttpTrigger1?code=bgsYSm-_PlxnRQ0EU0YvfespzQ021leJh57vaEmf-py0AzFu4KwoSw==", csvline);
                        
                        // use response body for further work if needed...
                        var responseBody = response.Content.ReadAsStringAsync();

                    }

                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
