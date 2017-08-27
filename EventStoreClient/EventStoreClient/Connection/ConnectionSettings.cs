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
        public readonly int CatchupSubscriptionTimeout;
        public readonly int MaxOperationRetries;
        public readonly int MaxReconnectAttempts;

        public ConnectionSettings(UserCredentials userCredentials, string hostAddress, int port, string connectionName, int catchupSubscriptionTimeout = 5000, int maxOperationRetries = -1, int maxReconnectAttempts = -1)
        {
            DefaultUserCredentials = userCredentials;
            HostAddress = hostAddress;
            Port = port;
            ConnectionName = connectionName;
            CatchupSubscriptionTimeout = catchupSubscriptionTimeout;
            MaxOperationRetries = maxOperationRetries;
            MaxReconnectAttempts = maxReconnectAttempts;
        }
    }
}
