## **Консьюмер.**
(ссылка на сервис Producer [https://github.com/MkhlLis/Producer/tree/master](https://github.com/MkhlLis/Producer/tree/master))

Взаимодействие между МС -- асинхронное через Apache Kafka


Интерфейсы связаны с реализациями посредством DI (Startup.cs).
```C#
        services.AddHostedService<Worker>();
        services.ConfigureKafka(configuration);
        services.AddSingleton<IMessageConsumer, MessageConsumer>();
```

**1. Сервис реализует MVP получения сообщений из Kafka**
Особенности:
1. Использован Confluent.Kafka.DependencyInjection, конфгурация консьюмера реализована через appsettings.json:
```JSON
"Kafka": {
    "Consumer": {
      "bootstrap.servers": "localhost:9092",
      "group.id": "example",
      "auto.offset.reset": "Earliest",
      "enable.auto.commit": "true"
    }
  }
```
```
services.Configure<ConsumerConfig>(configuration.GetSection("Kafka"));
services.AddKafkaClient();
```
2. HostedService Worker запускает потребитель до остановки
```C#
protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            }

            await _consumer.ConsumeAsync(stoppingToken);

            await Task.Delay(10000, stoppingToken);
        }
    }
```
3. MessageConsumer подписывается на топик и ожидает появления новых сообщений вычитывает их и сериализирует (Avro)
```C#
public async Task ConsumeAsync(CancellationToken cancellationToken)
    {
        _consumer.Subscribe("test-topic");  // Подписываемся на топик Kafka
        
        while (true)
        {
            try
            {
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
```

TODO: Inbox используя например KafkaFlow. 
