using EventStore.Client.Messages;
using Google.Protobuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventStoreClient.Connection
{
    internal partial class ConnectionManager
    {
#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task<CatchupSubscription> CreateCatchupSubscriptionAsync(string stream, long fromNumber, Func<ResolvedEvent, Task> eventAppearedCallback, int timeout)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            var catchupCorrelationId = Guid.NewGuid();
            var catchupSubscription = new CatchupSubscription(eventAppearedCallback, async () =>
            {
                // Remove subscription from dictionary and send package to drop subscription
                _catchupSubscriptions.TryRemove(catchupCorrelationId, out var _);
                var dropSubscriptionPackage = new TcpPackage(TcpCommand.UnsubscribeFromStream, catchupCorrelationId, null);
                _pendingSendMessages.Enqueue(dropSubscriptionPackage);
            }, catchupCorrelationId);

            if (_catchupSubscriptions.TryAdd(catchupCorrelationId, catchupSubscription))
            {
#pragma warning disable 4014
                // Launch a new task to handle the subscription startup
                Task.Run(async () =>
#pragma warning restore 4014
                {
                    // First, read all events on stream from position provided (since stream subscription only returns new events)
                    await ReadEvents(stream, fromNumber, long.MaxValue, true, catchupSubscription.HandleEvent, true).ConfigureAwait(false);

                    // Then start stream
                    var subscriptionMessage = new SubscribeToStream()
                    {
                        EventStreamId = stream,
                        ResolveLinkTos = true
                    };

                    // Serialize message
                    ArraySegment<byte> messageSerialized;
                    using (var memory = new MemoryStream())
                    {
                        subscriptionMessage.WriteTo(memory);
                        messageSerialized = new ArraySegment<byte>(memory.GetBuffer(), 0, (int)memory.Length);
                    }

                    var package = new TcpPackage(TcpCommand.SubscribeToStream, catchupCorrelationId, messageSerialized.ToArray<byte>());
                    _pendingSendMessages.Enqueue(package);

                    // Wait for subscription to start before returning it
                    if (await Task.WhenAny(catchupSubscription.SubscriptionStarted.Task, Task.Delay(timeout)) !=
                        catchupSubscription.SubscriptionStarted.Task)
                    {
                        // Timed out before completing subscription startup with server
                        await catchupSubscription.CloseSubscriptionAsync().ConfigureAwait(false);
                        throw new Exception("Failed to create stream subscription after completing initial event reads.");
                    }

                    // Read remaining events before subscription started
                    var streamStart = await catchupSubscription.SubscriptionStarted.Task.ConfigureAwait(false);
                    await ReadEvents(stream, Thread.VolatileRead(ref catchupSubscription.LastEventNumberHandled), streamStart - catchupSubscription.LastEventNumberHandled, true, catchupSubscription.HandleEvent, true).ConfigureAwait(false);

                    // Events handled up to where stream subscription started, so can start handling incoming subscription events now
                    catchupSubscription.CatchupToStreamCompleted();
                });
                return catchupSubscription;
            }
            throw new Exception("CorrelationId already in use?");
        }
    }
}
