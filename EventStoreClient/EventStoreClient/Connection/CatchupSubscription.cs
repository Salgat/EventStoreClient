using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventStoreClient
{
    internal class CatchupSubscription
    {
        private readonly Func<RecordedEvent, Task> _callback;
        private readonly ConcurrentQueue<IList<RecordedEvent>> _pendingEvents = new ConcurrentQueue<IList<RecordedEvent>>();
        private int _alive = 0; // 0 = alive, 1 = closed
        private readonly Func<Task> _closeSubscription;

        public CatchupSubscription(Func<RecordedEvent, Task> eventHandler, Func<Task> closeSubscriptionCallback)
        {
            _callback = eventHandler;
            _closeSubscription = closeSubscriptionCallback;
        }
        
        public async Task HandlePendingEvents()
        {
            if (_pendingEvents.TryDequeue(out var events))
            {
                foreach (var e in events)
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
        }
    }
}
