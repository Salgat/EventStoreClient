using EventStoreClient.Connection;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventStoreClient
{
    public sealed class ConnectionSettings
    {
        public readonly UserCredentials DefaultUserCredentials;
        public readonly string HostAddress;
        public readonly string ConnectionName;
        public readonly int Port;

        public ConnectionSettings(UserCredentials userCredentials, string hostAddress, int port, string connectionName)
        {
            DefaultUserCredentials = userCredentials;
            HostAddress = hostAddress;
            Port = port;
            ConnectionName = connectionName;
        }
    }
}
