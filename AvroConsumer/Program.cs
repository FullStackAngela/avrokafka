using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Avro.Generic;
using Npgsql;
using Confluent.Kafka.SyncOverAsync;
class Program 
{
    private const string KafkaBootstrapServers = "kafka:9092";
    private const string SchemaRegistryUrl = "http://schema-registry:8081";
      private const string UserTopicName = "avro_topic_user";
        private const string OrderTopicName = "avro_topic_order";

    private const string PostgresConnectionString = "Host=postgres;Username=postgres;Password=postgres;Database=avrodb";

    static async Task Main() 
    {
        var schemaRegistryConfig = new SchemaRegistryConfig { Url = SchemaRegistryUrl };
        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
         var config = new ConsumerConfig 
         {
            BootstrapServers = KafkaBootstrapServers,
            GroupId = "avro_group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
         };

         using var consumer = new ConsumerBuilder<string, GenericRecord>(config)
         .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
         .Build();

        consumer.Subscribe(new[] { UserTopicName, OrderTopicName });

         using var connection = new NpgsqlConnection(PostgresConnectionString);
         await connection.OpenAsync();

         while (true)
         {
            var result = consumer.Consume(CancellationToken.None);
            var schema = result.Message.Value.Schema;
            var tableName = schema.Name.ToLower();

            var createTableSQL = new StringBuilder($"CREATE TABLE IF NOT EXISTS {tableName} (");
            foreach(var field in ((Avro.RecordSchema)schema).Fields)
            {
                createTableSQL.Append($"{field.Name} {GetPostgresType(field.Schema)}, ");
            }
            createTableSQL.Length -= 2;
            createTableSQL.Append(");");
            Console.WriteLine(createTableSQL);

            await using var createTableCmd = new NpgsqlCommand(createTableSQL.ToString(), connection);
            await createTableCmd.ExecuteNonQueryAsync();

            var insertSQL = new StringBuilder($"INSERT INTO \"{tableName}\" (");
            var valueSQL = new StringBuilder(" VALUES (");
            foreach (var field in schema.Fields)
            {
                insertSQL.Append($"{field.Name}, ");
                valueSQL.Append($"'{result.Message.Value[field.Name]}', ");
            }
            insertSQL.Length -= 2;
            valueSQL.Length -= 2;
            insertSQL.Append(")");
            valueSQL.Append(");");
            Console.WriteLine(insertSQL);
            Console.WriteLine(valueSQL);

            await using var transaction = await connection.BeginTransactionAsync();
            await using var insertCmd = new NpgsqlCommand(insertSQL.ToString() + valueSQL.ToString(), connection);
            await insertCmd.ExecuteNonQueryAsync();
            await transaction.CommitAsync();

            Console.WriteLine($"Stored record in {tableName}");
         }
    }

    private static string GetPostgresType(Avro.Schema schema) => 
        schema.Tag switch 
        {
            Avro.Schema.Type.Int => "INTEGER",
            Avro.Schema.Type.String => "TEXT",
            Avro.Schema.Type.Double => "DOUBLE PRECISION",
            _ => "TEXT"
        };
}