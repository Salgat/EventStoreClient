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

        private ConnectionManager _connectionManager;

        public EventStoreConnection(ConnectionSettings settings)
        {
            Settings = settings;
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task CloseAsync()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            // Although not async, we make this Public method async due to the likelihood of future changes requiring it to be async
            _connectionManager.CloseConnection();
        }

        public async Task ConnectAsync()
        {
            _connectionManager = new ConnectionManager(Settings);
            await _connectionManager.StartConnection().ConfigureAwait(false);
        }

        public async Task WriteEvents(IEnumerable<CreateEvent> events, string stream, long expectedEventNumber)
        {
            await _connectionManager.WriteEvents(events, stream, expectedEventNumber).ConfigureAwait(false);
        }

        public Task<IEnumerable<RecordedEvent>> ReadEvents(string stream, long fromNumber, int count, bool resolveLinkTos)
        {
            return _connectionManager.ReadEvents(stream, fromNumber, count, resolveLinkTos);
        }
    }
}
