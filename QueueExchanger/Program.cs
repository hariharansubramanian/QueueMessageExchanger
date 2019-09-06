using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System;

namespace QueueExchanger
{
    class Program
    {
        private const string SRC_CONN_STRING = "UseDevelopmentStorage=true";
        private const string SRC_QUEUE_NAME = "blob-archiver-queue";
        private const string DEST_CONN_STRING = "UseDevelopmentStorage=true";
        private const string DEST_QUEUE_NAME = "blob-archiver-queue-poison";
        private const int BATCH_READ_COUNT = 20;
        private const int INVISIBLE_MINUTES_THRESHOLD = 5;
        static void Main(string[] args)
        {
            CloudStorageAccount sourceStorageAccount = CloudStorageAccount.Parse(SRC_CONN_STRING);
            CloudQueueClient sourceQueueClient = sourceStorageAccount.CreateCloudQueueClient();
            CloudQueue sourceQueue = sourceQueueClient.GetQueueReference(SRC_QUEUE_NAME);


            CloudStorageAccount destinationStorageAccount = CloudStorageAccount.Parse(DEST_CONN_STRING);
            CloudQueueClient destinationQueueClient = destinationStorageAccount.CreateCloudQueueClient();
            CloudQueue destinationQueue = destinationQueueClient.GetQueueReference(DEST_QUEUE_NAME);
            try
            {
                destinationQueue.CreateIfNotExistsAsync().GetAwaiter().GetResult();
                sourceQueue.FetchAttributesAsync().GetAwaiter().GetResult();
                int? srcQueueMessageCount = sourceQueue.ApproximateMessageCount;
                Console.WriteLine($"Number of messages in {SRC_QUEUE_NAME}: {srcQueueMessageCount}");

                while (srcQueueMessageCount > 0)
                {
                    foreach (CloudQueueMessage message in sourceQueue.GetMessagesAsync(BATCH_READ_COUNT, TimeSpan.FromMinutes(INVISIBLE_MINUTES_THRESHOLD), null, null).GetAwaiter().GetResult())
                    {
                        Console.WriteLine($"Processing {BATCH_READ_COUNT} messages from {SRC_QUEUE_NAME}.");
                        var cloudMessage = new CloudQueueMessage(message.AsString);
                        destinationQueue.AddMessageAsync(cloudMessage, TimeSpan.MaxValue, TimeSpan.FromSeconds(0), null, null).GetAwaiter().GetResult();
                        sourceQueue.DeleteMessageAsync(message).GetAwaiter().GetResult();
                    }
                    sourceQueue.FetchAttributesAsync().GetAwaiter().GetResult();
                    srcQueueMessageCount = sourceQueue.ApproximateMessageCount;
                    Console.WriteLine($"Number of messages in {SRC_QUEUE_NAME}: {srcQueueMessageCount}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.StackTrace);
            }

        }
    }
}
