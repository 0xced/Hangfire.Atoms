using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Net;
using Ductus.FluentDocker;
using Ductus.FluentDocker.Model.Builders;
using Ductus.FluentDocker.Model.Containers;
using Ductus.FluentDocker.Services;
using Ductus.FluentDocker.Services.Extensions;
using Npgsql;

namespace Hangfire.Atoms.Tests.Web;

public sealed class PostgresContainer : IDisposable
{
    private readonly string _dockerImageName;
    private const string Password = "hunter2";
    private const int Port = 5432;
    private static readonly string PortAndProto = $"{Port}/tcp";

    private IContainerService _container;
    private string _connectionString;

    public PostgresContainer(string dockerImageName)
    {
        _dockerImageName = dockerImageName ?? throw new ArgumentNullException(nameof(dockerImageName));
        _connectionString = Environment.GetEnvironmentVariable("Hangfire_PostgreSql_ConnectionString");
    }

    public string GetConnectionString()
    {
        if (_connectionString == null)
        {
            Console.WriteLine($"Starting a {_dockerImageName} docker container...");
            // TODO: start existing container if found
            //var existingContainer = Fd.Hosts().Native().GetContainers().FirstOrDefault(e => e.Name == "Hangfire.Atoms.Tests.Web");
            //existingContainer?.Start();
            _container = Fd.Build()
                .UseContainer()
                .UseImage(_dockerImageName)
                .WithName("Hangfire.Atoms.Tests.Web")
                .MountVolume("Hangfire.Atoms.Tests.Web", "/var/lib/postgresql/data", MountType.ReadWrite)
                .UseCustomResolver(ResolveHostEndpoint)
                .ExposePort(Port)
                .WithEnvironment($"POSTGRES_PASSWORD={Password}")
                .Wait("", CheckDatabaseConnection)
                .Build()
                .Start();

            _connectionString = GetConnectionString(_container);
        }

        return _connectionString;
    }

    private static string GetConnectionString(IContainerService container)
    {
        var endPoint = container.ToHostExposedEndpoint(PortAndProto) ?? throw new InvalidOperationException("Failed to get host endpoint.");
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = endPoint.Address.ToString(),
            Port = endPoint.Port,
            Username = "postgres",
            Password = Password,
            SearchPath = "atoms",
        };
        return builder.ConnectionString;
    }

    private static int CheckDatabaseConnection(IContainerService container, int count)
    {
        var connectionString = GetConnectionString(container);
        using var connection = new NpgsqlConnection(connectionString);
        try
        {
            connection.Open();
            return 0;
        }
        catch (DbException exception)
        {
            if (count > 10)
                throw new TimeoutException($"Timeout waiting for database at \"{connectionString}\"", exception);
            return 500; // ms
        }
    }

    private static IPEndPoint ResolveHostEndpoint(Dictionary<string, HostIpEndpoint[]> ports, string portAndProto, Uri dockerUri)
    {
        if (ports == null || !ports.TryGetValue(portAndProto, out var endpoints) || endpoints == null || endpoints.Length == 0)
            return null;

        var endpoint = endpoints[0];
        var address = dockerUri != null && IPAddress.TryParse(dockerUri.Host, out var parsedAddress) ? parsedAddress : Equals(endpoint.Address, IPAddress.Any) ? IPAddress.Loopback : endpoint.Address;
        return new IPEndPoint(address, endpoint.Port);
    }

    public void Dispose()
    {
        _container?.Dispose();
    }
}