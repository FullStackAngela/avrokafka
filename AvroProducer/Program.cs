using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Avro.Generic;

namespace AvroProducer 
{
    class Program 
    {
        private const string KafkaBootstrapServers = "kafka:9092";
        private const string SchemaRegistryUrl = "http://schema-registry:8081";
        private const string UserTopicName = "avro_topic_user";
        private const string OrderTopicName = "avro_topic_order";


        static  async Task Main()
        {
             var schemaRegistryConfig = new SchemaRegistryConfig { Url = SchemaRegistryUrl };
            using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);

            var config = new ProducerConfig {BootstrapServers = KafkaBootstrapServers };

            using var producer = new ProducerBuilder<string, GenericRecord>(config)
            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
            .Build();

            var schemas = new[] {
                "{\"type\":\"record\",\"name\":\"Users\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}",
                "{\"type\":\"record\",\"name\":\"Orders\",\"fields\":[{\"name\":\"order_id\",\"type\":\"int\"},{\"name\":\"amount\",\"type\":\"double\"}]}"
            };

            foreach(var schemaJson in schemas){
                var schema = Avro.Schema.Parse(schemaJson);
                var record = new GenericRecord((Avro.RecordSchema)schema);

                if (schema.Name == "Users"){
                    record.Add("id",1);
                    record.Add("name", "John Doe");
                    await producer.ProduceAsync(UserTopicName, new Message<string, GenericRecord> { Key = schema.Name, Value = record });

                } else {
                    record.Add("order_id", 1001);
                    record.Add("amount", 250.75);
                    await producer.ProduceAsync(OrderTopicName, new Message<string, GenericRecord> { Key = schema.Name, Value = record });

                }
                Console.WriteLine($"Produced message with schema: {schema.Name}");
            }
        }
    }
}