using Avro.Specific;

namespace Consumer.Interfaces;

public interface IMessageConsumer
{
    Task ConsumeAsync(CancellationToken cancellationToken);
}