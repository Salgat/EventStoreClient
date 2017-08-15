using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace EventStoreClient.Connection
{
    public class EventStoreConnection : IEventStoreConnection
    {
        public string ConnectionName => Settings.ConnectionName;

        public ConnectionSettings Settings { get; }

        private ConnectionManager connectionManager;

        public EventStoreConnection(ConnectionSettings settings)
        {
            Settings = settings;
        }

        public async Task CloseAsync()
        {
            await connectionManager.CloseConnectionAsync().ConfigureAwait(false);
        }

        public async Task ConnectAsync()
        {
            connectionManager = new ConnectionManager(Settings);
            await connectionManager.StartConnection().ConfigureAwait(false);
        }
    }
}
