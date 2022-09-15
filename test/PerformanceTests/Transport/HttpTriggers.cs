namespace PerformanceTests.Transport
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using System.Net;
    using Dynamitey.DynamicObjects;
    using System.Collections.Generic;
    using System.Web.Http;
    using DurableTask.Netherite;
    using System.Linq;

    public static class TransportHttp
    {
        static IActionResult ErrorResult(Exception exception, string context, ILogger logger)
        {
            logger.LogError(exception, $"exception in {context}");
            return new ObjectResult($"exception in {context}: {exception}") { StatusCode = (int)HttpStatusCode.InternalServerError };
        }

        [FunctionName(nameof(StartAll))]
        public static async Task<IActionResult> StartAll(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "triggertransport/startall")] HttpRequest req,
            //ITransportLayerFactory transportFactory,
            ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                string[] hosts = JsonConvert.DeserializeObject<string[]>(requestBody);
                //TriggerTransport transport = ((TriggerTransportFactory)transportFactory).Instance;
                TriggerTransport transport = TriggerTransportFactory.Instance;
                await transport.StartAllAsync(hosts);
                return new OkResult();
            }
            catch (Exception e)
            {
                return ErrorResult(e, nameof(StartAll), log);
            }
        }

        [FunctionName(nameof(StartLocal))]
        public static async Task<IActionResult> StartLocal(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "triggertransport/startlocal/{index}")] HttpRequest req,
            int index,
            //ITransportLayerFactory transportFactory,
            ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                string[] hosts = JsonConvert.DeserializeObject<string[]>(requestBody);
                //TriggerTransport transport = ((TriggerTransportFactory)transportFactory).Instance;
                TriggerTransport transport = TriggerTransportFactory.Instance;
                await transport.StartLocalAsync(hosts, index);
                return new OkResult();
            }
            catch (Exception e)
            {
                return ErrorResult(e, nameof(StartLocal), log);
            }
        }

        [FunctionName(nameof(TestAddress))]
        public static async Task<IActionResult> TestAddress(
           [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "triggertransport/test")] HttpRequest req,
           //ITransportLayerFactory transportFactory,
           ILogger log)
        {
            try
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                string[] hosts = JsonConvert.DeserializeObject<string[]>(requestBody);
                //TriggerTransport transport = ((TriggerTransportFactory)transportFactory).Instance;
                TriggerTransport transport = TriggerTransportFactory.Instance;
                await transport.WhenOrchestrationServiceStarted;
                var tasks = new List<Task<string>>();
                for (int i = 0; i < hosts.Length; i++)
                {
                    tasks.Add(transport.Test(hosts[i]));
                }
                await Task.WhenAll(tasks);
                ;
                return new OkObjectResult(string.Join('\n', tasks.Select(t => t.Result)));
            }
            catch (Exception e)
            {
                return ErrorResult(e, nameof(StartAll), log);
            }
        }

        [FunctionName(nameof(GetClientId))]
        public static async Task<IActionResult> GetClientId(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "triggertransport/client")] HttpRequest req,
            //ITransportLayerFactory transportFactory,
            ILogger log)
        {
            try
            {
                //TriggerTransport transport = ((TriggerTransportFactory)transportFactory).Instance;
                TriggerTransport transport = TriggerTransportFactory.Instance;
                await transport.WhenOrchestrationServiceStarted;
                return new OkObjectResult(new { transport.ClientId });
            }
            catch (Exception e)
            {
                return ErrorResult(e, nameof(GetClientId), log);
            }
        }

        [FunctionName(nameof(DeliverToPartition))]
        public static async Task<IActionResult> DeliverToPartition(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "triggertransport/partition/{partitionId}")] HttpRequest req,
            int partitionId,
            //ITransportLayerFactory transportFactory,
            ILogger log)
        {
            try
            {
                //TriggerTransport transport = ((TriggerTransportFactory)transportFactory).Instance;
                TriggerTransport transport = TriggerTransportFactory.Instance;
                await transport.WhenLocallyStarted;
                bool success = await transport.DeliverToLocalPartitionAsync(partitionId, req.Body);
                if (!success)
                {
                    return new NotFoundResult();
                }
                else
                {
                    return new OkResult();
                }
            }
            catch(Exception e)
            {
                return ErrorResult(e, nameof(DeliverToPartition), log);
            }
        }

        [FunctionName(nameof(DeliverToClient))]
        public static async Task<IActionResult> DeliverToClient(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "triggertransport/client/{clientId}")] HttpRequest req,
            Guid clientId,
            //ITransportLayerFactory transportFactory,
            ILogger log)
        {
            try
            {
                //TriggerTransport transport = ((TriggerTransportFactory)transportFactory).Instance;
                TriggerTransport transport = TriggerTransportFactory.Instance;
                await transport.WhenLocallyStarted;
                bool success = transport.DeliverToLocalClient(clientId, req.Body);
                if (!success)
                {
                    return new NotFoundResult();
                }
                else
                {
                    return new OkResult();
                }
            }
            catch (Exception e)
            {
                return ErrorResult(e, nameof(DeliverToClient), log);
            }
        }

        [FunctionName(nameof(DeliverToLoadMonitor))]
        public static async Task<IActionResult> DeliverToLoadMonitor(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "triggertransport/loadmonitor")] HttpRequest req,
            //ITransportLayerFactory transportFactory,
            ILogger log)
        {
            try
            {
                //TriggerTransport transport = ((TriggerTransportFactory)transportFactory).Instance;
                TriggerTransport transport = TriggerTransportFactory.Instance;
                await transport.WhenLocallyStarted;
                bool success = transport.DeliverToLocalLoadMonitor(req.Body);
                if (!success)
                {
                    return new NotFoundResult();
                }
                else
                {
                    return new OkResult();
                }
            }
            catch (Exception e)
            {
                return ErrorResult(e, nameof(DeliverToLoadMonitor), log);
            }
        }
    }
}
