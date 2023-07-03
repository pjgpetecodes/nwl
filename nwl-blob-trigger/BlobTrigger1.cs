using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.WebJobs.ServiceBus;
using Azure.Messaging.ServiceBus;
using System.Text;

namespace nwl.blobtrigger
{
    public class BlobTrigger1
    {
        [FunctionName("BlobTrigger1")]
        
        public static void Run([BlobTrigger("uploads/{name}", Source = BlobTriggerSource.EventGrid, Connection = "nwlstorage1_STORAGE")]Stream myBlob, String name, 
                                [ServiceBus("%ServiceBusQueueName%", Connection = "ServiceBusConnection", EntityType = ServiceBusEntityType.Queue)]out string queueMessage, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            queueMessage = GetBlobInfo(myBlob, name);

        }

        public static string GetBlobInfo(Stream myBlob, string name)
        {
            string blobInfo = "";
            blobInfo += "Blob Name: " + name + "\n";
            blobInfo += "Blob Size: " + myBlob.Length + " Bytes\n";
            
            return blobInfo;
        }
    }
}
