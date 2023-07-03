using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.EventHubs;
using Azure.Messaging.EventHubs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Threading.Tasks;

namespace nwl.chunker
{
    public static class ServiceBusQueueTrigger1
    {
        [FunctionName("ServiceBusQueueTrigger1")]
         public static async Task Run([ServiceBusTrigger("nwlnewfilequeue", Connection = "nwlsb1_SERVICEBUS")]string myQueueItem, 
                            [EventHub("%nwleventhub1_azurefunctionsender_EVENTHUB_name%", Connection = "nwleventhub1_RootManageSharedAccessKey_EVENTHUB")] IAsyncCollector<Azure.Messaging.EventHubs.EventData> outputEvents,
                            ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");

            string storageAccountConnectionString = Environment.GetEnvironmentVariable("nwlstorage1_STORAGE");            

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageAccountConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("uploads");

            CloudBlockBlob blockBlob = container.GetBlockBlobReference(myQueueItem);


            

            // Queue the message to be sent in the background by adding it to the collector.
            // If only the event is passed, an Event Hub partition to be be assigned via
            // round-robin for each batch.
            //await outputEvents.AddAsync(new EventData(newEventBody));

        }
    }
}
