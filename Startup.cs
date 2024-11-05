using Confluent.Kafka;
using Consumer.Implementation;
using Consumer.Interfaces;

namespace Consumer;

public static class Startup
{
    public static IHost IntitializeApp(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        
        // build configuration
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory().Replace("/bin/Debug/net8.0", ""))
            .AddJsonFile($"appsettings.{builder.Environment.EnvironmentName}.json", false)
            .Build();
        
        Configure(builder.Services, configuration);

        var host = builder.Build();
        return host;
    }
    
    private static IServiceCollection Configure(IServiceCollection services, IConfiguration configuration)
    {
        services.AddHostedService<Worker>();
        
        services.ConfigureKafka(configuration);
        services.AddSingleton<IMessageConsumer, MessageConsumer>();
        
        return services;
    }

    private static void ConfigureKafka(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<ConsumerConfig>(configuration.GetSection("Kafka"));
        services.AddKafkaClient();
    }
}