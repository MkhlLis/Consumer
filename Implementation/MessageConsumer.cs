using System.Reflection;
using avro;
using Avro.IO;
using Avro.Specific;
using Confluent.Kafka;
using Consumer.Interfaces;

namespace Consumer.Implementation;

public class MessageConsumer : IMessageConsumer
{
    private readonly IConsumer<Ignore, byte[]> _consumer;

    public MessageConsumer(IConsumer<Ignore, byte[]> consumer)
    {
        _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
    }
        
    public async Task ConsumeAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                _consumer.Subscribe("test-topic");  // Подписываемся на топик Kafka
                
                // Получение сообщения из Kafka
                var consumeResult = _consumer.Consume(cancellationToken);

                if (consumeResult.Message.Value.Length > 0)
                {
                    // Десериализация сообщения вручную с использованием Avro
                    var user = DeserializeAvroMessage(consumeResult.Message.Value);

                    if (user != default)
                    {
                        // Вывод данных пользователя
                        Console.WriteLine(
                            $"Received User: ID = {user.id}, Name = {user.name}, Email = {user.email ?? "null"}");
                    }
                }

                // Фиксируем смещение после успешной обработки сообщения
                _consumer.Commit(consumeResult);

                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Consume error: {e.Error.Reason}");
            }
        }
    }
    
    // Метод для десериализации Avro-сообщения
    static User DeserializeAvroMessage(byte[] avroData)
    {
        try
        {
            // Определяем схему User
            var userSchema = User._SCHEMA;

            // Десериализация Avro-сообщения из двоичных данных
            using (var stream = new MemoryStream(avroData))
            {
                var reader = new BinaryDecoder(stream);
                var datumReader = new SpecificDatumReader<User>(userSchema, userSchema);

                // Чтение и десериализация данных
                return datumReader.Read(null, reader);
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            return default;
        }
    }
}