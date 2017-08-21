using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Client.Messages;

namespace EventStoreClient.Connection
{
    public class EventStoreConnection : IEventStoreConnection
    {
        public string ConnectionName => Settings.ConnectionName;
        private int started = 0; // 0 == false, 1 == true

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
            await _connectionManager.CloseConnectionAsync().ConfigureAwait(false);
        }

        public async Task ConnectAsync()
        {
            _connectionManager = new ConnectionManager(Settings);
            await _connectionManager.StartConnection().ConfigureAwait(false);
            Interlocked.Increment(ref started);
        }

        public async Task WriteEvents(IEnumerable<CreateEvent> events, string stream, long expectedEventNumber)
        {
            if (started == 0) throw new Exception("Connection not started yet.");
            await _connectionManager.WriteEvents(events, stream, expectedEventNumber).ConfigureAwait(false);
        }

        public Task<IEnumerable<RecordedEvent>> ReadEvents(string stream, long fromNumber, int count, bool resolveLinkTos)
        {
            if (started == 0) throw new Exception("Connection not started yet.");
            return _connectionManager.ReadEvents(stream, fromNumber, count, resolveLinkTos);
        }

        public Task<CatchupSubscription> CreateCatchupSubscription(string stream, long fromNumber, Func<ResolvedEvent, Task> eventAppearedCallback)
        {
            if (started == 0) throw new Exception("Connection not started yet.");
            return _connectionManager.CreateCatchupSubscriptionAsync(stream, fromNumber, eventAppearedCallback, Settings.CatchupSubscriptionTimeout);
        }
    }
}
