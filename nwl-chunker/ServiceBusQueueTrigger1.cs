using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.EventHubs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Threading.Tasks;
using CsvHelper;

namespace nwl.chunker
{
    public static class ServiceBusQueueTrigger1
    {
        [FunctionName("ServiceBusQueueTrigger1")]
         public static async Task Run([ServiceBusTrigger("%ServiceBusQueueName%", Connection = "nwlsb1_SERVICEBUS", AutoComplete = true)]string myQueueItem, int deliveryCount, MessageReceiver messageReceiver, string lockToken, 
                            [EventHub("%nwleventhub1_azurefunctionsender_EVENTHUB_name%", Connection = "nwleventhub1_RootManageSharedAccessKey_EVENTHUB")] IAsyncCollector<Azure.Messaging.EventHubs.EventData> outputEvents,
                            ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");

            string storageAccountConnectionString = Environment.GetEnvironmentVariable("nwlstorage1_STORAGE");            

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageAccountConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("uploads");

            // parse out the blob name from "Blob Name: AllData.csv\nBlob Size: 1793704 Bytes\n"
            string blobName = myQueueItem.Substring(11, myQueueItem.IndexOf("\n") - 11);

            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

            int NumberOfRows = 0;

            // Use the CSV Helper library to read the CSV file
            using (var stream = await blockBlob.OpenReadAsync())
            using (var reader = new System.IO.StreamReader(stream))
            using (var csv = new CsvReader(reader, System.Globalization.CultureInfo.InvariantCulture))
            {

                
                // Read the CSV file and create a new CsvLine object for each line
                while (csv.Read())
                {
                    var csvLine = csv.GetRecord<CsvLine>();

                    // Create a new EventData object from the CsvLine object
                    var newEventBody = new EventData(System.Text.Json.JsonSerializer.Serialize(csvLine));

                    // Queue the message to be sent in the background by adding it to the collector.
                    // If only the event is passed, an Event Hub partition to be be assigned via
                    // round-robin for each batch.
                    await outputEvents.AddAsync(newEventBody);

                    NumberOfRows++;
                }
            }      

            log.LogInformation($"Successfully processed {NumberOfRows.ToString()} rows from {blobName}");

            // Queue the message to be sent in the background by adding it to the collector.
            // If only the event is passed, an Event Hub partition to be be assigned via
            // round-robin for each batch.
            //await outputEvents.AddAsync(new EventData(newEventBody));

        }
    }
}
