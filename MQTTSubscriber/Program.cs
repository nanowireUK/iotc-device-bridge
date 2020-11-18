using System;
using System.Text;
using System.Threading;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using System.Net.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using DotNetEnv;

namespace MQTTSubscriber
{
    class Program
    {
        static MqttFactory factory = new MqttFactory();
        static bool debug = true;        

        static void Main(string[] args)
        {
            DotNetEnv.Env.Load();
            String ttnServer = System.Environment.GetEnvironmentVariable("ttnServer");
            String ttnSClientId = System.Environment.GetEnvironmentVariable("ttnSClientId");
            String ttnAppId = System.Environment.GetEnvironmentVariable("ttnAppId");
            String ttnAppPassword = System.Environment.GetEnvironmentVariable("ttnAppPassword");
            String azureFunctionUrl = System.Environment.GetEnvironmentVariable("azureFunctionUrl");
            String azureFunctionPath = System.Environment.GetEnvironmentVariable("azureFunctionPath");
            String adtModelId = System.Environment.GetEnvironmentVariable("adtModelId");
            Console.WriteLine("test" + ttnSClientId);
            var options = new MqttClientOptionsBuilder()
                .WithClientId(ttnSClientId)
                .WithTcpServer(ttnServer, 8883)
                .WithCredentials(ttnAppId, ttnAppPassword)
                .WithTls()
                .WithCleanSession()
                .Build();
            
            var mqttClient = factory.CreateMqttClient();

            Console.WriteLine("Attempting Connection");
            mqttClient.ConnectAsync(options, CancellationToken.None);

            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("### CONNECTED WITH SERVER ###");
                var topic = new MqttTopicFilterBuilder().WithTopic($"{ttnAppId}/devices/+/up").Build();
                Console.WriteLine(topic.ToString());
                // Subscribe to a topic
                await mqttClient.SubscribeAsync(topic);

                Console.WriteLine("### SUBSCRIBED ###");
            });

            mqttClient.UseApplicationMessageReceivedHandler(async e =>
            {
                if (debug)
                {
                    Console.WriteLine("### RECEIVED APPLICATION MESSAGE ###");
                    Console.WriteLine($"+ Topic = {e.ApplicationMessage.Topic}");
                    Console.WriteLine($"+ Payload = {Encoding.UTF8.GetString(e.ApplicationMessage.Payload)}");
                    Console.WriteLine($"+ QoS = {e.ApplicationMessage.QualityOfServiceLevel}");
                    Console.WriteLine($"+ Retain = {e.ApplicationMessage.Retain}");
                    Console.WriteLine();
                }
                
                dynamic json = JsonConvert.DeserializeObject(Encoding.UTF8.GetString(e.ApplicationMessage.Payload));
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(azureFunctionUrl);
                    
                    var measurements = new
                    {
                        temperature = 25.6,
                        pressure = 75,
                        humidity = 13.5
                    };
                    var content = JObject.FromObject(new
                    {
                        hardware_serial = json.dev_id,
                        payload_fields = measurements,
                        modelId = adtModelId
                    });
                    var response = await client.PostAsync(azureFunctionPath,
                                  new StringContent(JsonConvert.SerializeObject(content),
                                  Encoding.UTF8, "application/json"));
                    
                    response.EnsureSuccessStatusCode();

                    Console.WriteLine(await response.Content.ReadAsStringAsync());
                }

            });

            Console.ReadKey();
        }
    }
}
