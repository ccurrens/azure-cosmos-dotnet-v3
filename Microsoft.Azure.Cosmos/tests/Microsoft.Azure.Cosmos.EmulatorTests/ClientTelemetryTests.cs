namespace Microsoft.Azure.Cosmos.SDK.EmulatorTests
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;
    using HdrHistogram;
    using Microsoft.Azure.Cosmos.CosmosElements;
    using Microsoft.Azure.Cosmos.CosmosElements.Telemetry;
    using Microsoft.Azure.Cosmos.Fluent;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ClientTelemetryTests
    {
        [TestMethod]
        public async Task TestAllDBOperationsIfTelemetryIsEmabled()
        {
            CosmosClientBuilder cosmosClientBuilder = TestCommon.GetDefaultConfiguration();
            cosmosClientBuilder.WithTelemetryEnabled();

            string databaseId = Guid.NewGuid().ToString();
            string containerId = Guid.NewGuid().ToString();
            using (CosmosClient cosmosClient = cosmosClientBuilder.Build())
            {
                Database database = await cosmosClient.CreateDatabaseAsync(databaseId);
                Container container = await database.CreateContainerAsync(
                    containerId,
                    "/id");

                // Create an item
                var testItem = new { id = "MyTestItemId", partitionKeyPath = "MyTestPkValue", details = "it's working", status = "done" };
                await container.CreateItemAsync(testItem);

                ClientTelemetryInfo telemetryInfo = cosmosClient.DocumentClient.clientTelemetry.clientTelemetryInfo;

                Assert.IsNull(telemetryInfo.AcceleratedNetworking);
                Assert.IsNotNull(telemetryInfo.ClientId);
                Assert.IsNotNull(telemetryInfo.GlobalDatabaseAccountName);
                Assert.IsNotNull(telemetryInfo.UserAgent);
                Assert.AreEqual(telemetryInfo.OperationInfoMap.Count, 2);

                List<string> allowedMetrics 
                    = new List<string>(new string[] { 
                        ClientTelemetry.RequestChargeName, 
                        ClientTelemetry.RequestLatencyName 
                    });
                List<string> allowedUnitnames = new List<string>(new string[] { ClientTelemetry.RequestChargeUnit, ClientTelemetry.RequestLatencyUnit });

                foreach (KeyValuePair<ReportPayload, LongConcurrentHistogram> entry in telemetryInfo.OperationInfoMap)
                {
                    Assert.AreEqual(entry.Key.Operation, Documents.OperationType.Create);
                    Assert.AreEqual(entry.Key.Resource, Documents.ResourceType.Document);
                    Assert.IsTrue(allowedMetrics.Contains(entry.Key.MetricInfo.MetricsName));
                    Assert.IsTrue(allowedUnitnames.Contains(entry.Key.MetricInfo.UnitName));
                }

                List<Documents.OperationType> allowedOperations 
                    = new List<Documents.OperationType>(new Documents.OperationType[] {
                        Documents.OperationType.Create,
                        Documents.OperationType.Read
                    });

                //Read an Item
                object p = await container.ReadItemAsync<dynamic>(testItem.id, new PartitionKey(testItem.id));
                Assert.AreEqual(telemetryInfo.OperationInfoMap.Count, 4);

                foreach (KeyValuePair<ReportPayload, LongConcurrentHistogram> entry in telemetryInfo.OperationInfoMap)
                {
                    Assert.IsTrue(allowedOperations.Contains(entry.Key.Operation));
                    Assert.AreEqual(entry.Key.Resource, Documents.ResourceType.Document);
                    Assert.IsTrue(allowedMetrics.Contains(entry.Key.MetricInfo.MetricsName));
                    Assert.IsTrue(allowedUnitnames.Contains(entry.Key.MetricInfo.UnitName));
                }
            }

        }
    }
}
