using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.Client.Messages;

namespace EventStoreClient
{
    public class CatchupSubscription
    {
        private readonly Guid _correlationId;
        private readonly Func<ResolvedEvent, Task> _callback;
        private readonly ConcurrentQueue<ResolvedEvent> _pendingEvents = new ConcurrentQueue<ResolvedEvent>();
        private int _alive = 0; // 0 = alive, 1 = closed
        private readonly Func<Task> _closeSubscription;
        internal readonly TaskCompletionSource<long> SubscriptionStarted = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        internal long LastEventNumberHandled = -1;
        private long _catchupToStreamCompleted = 0; // 0 = not finished, 1 = finished and can start handling stream events

        internal CatchupSubscription(Func<ResolvedEvent, Task> eventHandler, Func<Task> closeSubscriptionCallback, Guid correlationId)
        {
            _callback = eventHandler;
            _closeSubscription = closeSubscriptionCallback;
            _correlationId = correlationId;
        }

        internal void AddEvent(ResolvedEvent e)
        {
            _pendingEvents.Enqueue(e);
        }

        internal async Task HandlePendingEvents()
        {
            if (Thread.VolatileRead(ref _catchupToStreamCompleted) == 0) return;

            while (_pendingEvents.TryDequeue(out var e))
            {
                if (Thread.VolatileRead(ref _alive) >= 1) return;
                await HandleEvent(e).ConfigureAwait(false);
            }
        }

        internal async Task HandleEvent(ResolvedEvent e)
        {
            try
            {
                await _callback(e).ConfigureAwait(false);
                Interlocked.Exchange(ref LastEventNumberHandled, e.Event.EventNumber);
            }
            catch (Exception)
            {
                // Event handling failed, close subscription and rethrow exception.
                Interlocked.Increment(ref _alive);
                await _closeSubscription().ConfigureAwait(false);
                throw;
            }
        }

        /// <summary>
        /// Indicates that the catchup subscription can start handling pending incoming events from the stream.
        /// This is required since we have to handle all previous events up to the point where the subscription 
        /// started.
        /// </summary>
        internal void CatchupToStreamCompleted()
        {
            Interlocked.Increment(ref _catchupToStreamCompleted);
        }

        /// <summary>
        /// Closes the catchup subscription, ending handling of new events.
        /// </summary>
        /// <returns></returns>
        public async Task CloseSubscriptionAsync()
        {
            Interlocked.Increment(ref _alive);
            await _closeSubscription().ConfigureAwait(false);
        }
    }
}
