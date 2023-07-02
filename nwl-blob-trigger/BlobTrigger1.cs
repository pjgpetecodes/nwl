using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace nwl.blobtrigger
{
    public class BlobTrigger1
    {
        [FunctionName("BlobTrigger1")]
        public static void Run([BlobTrigger("uploads/{name}", Source = BlobTriggerSource.EventGrid, Connection = "nwlstorage1_STORAGE")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
        }
    }
}
