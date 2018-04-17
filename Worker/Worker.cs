using System;
using System.Threading;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Worker
{
    class Worker
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "task_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                    Console.WriteLine(" [*] Waiting for messages.");
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                     {
                         var body = ea.Body;
                         var message = Encoding.UTF8.GetString(body);
                         Console.WriteLine(" [X] Recieved {0}", message);
                         int dots = message.Split('.').Length - 1;
                         Thread.Sleep(1000 * dots);
                         Console.WriteLine(" [X] Done");
                         channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                     };
                    channel.BasicConsume(queue: "task_queue", autoAck: false, consumer: consumer);
                    Console.WriteLine(" Press [enter] to exit.");
                    Console.ReadLine();
                }
            }
        }
    }
}
