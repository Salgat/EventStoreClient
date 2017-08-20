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
            while (_pendingEvents.TryDequeue(out var e))
            {
                if (Thread.VolatileRead(ref _alive) >= 1) return;
                try
                {
                    await _callback(e).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Event handling failed, close subscription and rethrow exception.
                    Interlocked.Increment(ref _alive);
                    await _closeSubscription().ConfigureAwait(false);
                    throw;
                }
            }
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
