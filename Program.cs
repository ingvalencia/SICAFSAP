using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console(
        outputTemplate:
        "[{Timestamp:yyyy-MM-dd HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}"
    )
    .WriteTo.File(
        "Logs/sicafsap-.log",
        rollingInterval: RollingInterval.Day,
        retainedFileCountLimit: 30,
        shared: true
    )
    .CreateLogger();

try
{
    Console.Title = "Sistema de Inventarios | Servicio SAP";

    IHost host = Host.CreateDefaultBuilder(args)
        .UseWindowsService()
        .UseSerilog()
        .ConfigureServices(services =>
        {
            // ================================
            // CONFIGURACIÓN CRÍTICA
            // ================================
            services.Configure<HostOptions>(options =>
            {
                // Evita que el servicio se detenga si el Worker falla
                options.BackgroundServiceExceptionBehavior =
                    BackgroundServiceExceptionBehavior.Ignore;
            });

            // ================================
            // DEPENDENCIAS
            // ================================
            services.AddSingleton<SapService>();

            // El Worker depende de SapService
            services.AddHostedService<Worker>();
        })
        .Build();

    Log.Information("Sistema de Inventarios - Servicio SAP iniciado correctamente");

    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Sistema de Inventarios - Falla crítica al iniciar");
}
finally
{
    Log.CloseAndFlush();
}
