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

        public async Task WriteEvents(IEnumerable<CreateEvent> events, string stream, long expectedEventNumber)
        {
            await connectionManager.WriteEvents(events, stream, expectedEventNumber).ConfigureAwait(false);
        }

        public Task<IEnumerable<RecordedEvent>> ReadEvents(string stream, long fromNumber, int count, bool resolveLinkTos)
        {
            return connectionManager.ReadEvents(stream, fromNumber, count, resolveLinkTos);
        }
    }
}
