using EventStore.Client.Messages;
using Google.Protobuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventStoreClient.Connection
{
    internal partial class ConnectionManager
    {
        /// <summary>
        /// Reads events from a provided stream.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="fromNumber"></param>
        /// <param name="count"></param>
        /// <param name="resolveLinkTos"></param>
        /// <param name="eventHandlerCallback">Executes the callback against the read events.</param>
        /// <param name="discardEvents">Does not keep the events in memory after reading them. Useful for running callbacks against stream events where the result doesn't matter, especially on larger streams.</param>
        /// <returns></returns>
        public async Task<IEnumerable<RecordedEvent>> ReadEvents(string stream, long fromNumber, long count, bool resolveLinkTos, Func<ResolvedEvent, Task> eventHandlerCallback = null, bool discardEvents = false)
        {
            const int batchSize = 512;
            var result = new List<RecordedEvent>();
            long currentFromNumber = fromNumber;
            long lastEventNumber;
            try
            {
                lastEventNumber = checked(fromNumber + count - 1);
            }
            catch (OverflowException)
            {
                // We know a stream can only hold long.MaxValue, so we just set to end of stream
                lastEventNumber = long.MaxValue;
            }
            while (currentFromNumber <= lastEventNumber)
            {
                // TODO: Update batch size to be configurable
                var batchResult = (await ReadEventsBatch(stream, currentFromNumber, batchSize, resolveLinkTos, eventHandlerCallback).ConfigureAwait(false)).ToList();
                if (!discardEvents) result.AddRange(batchResult);

                // If this read resulted in no events, we are done reading
                if (!batchResult.Any()) break;

                // Continue to next batch of events
                currentFromNumber += batchSize;
            }

            return result;
        }

        private async Task<IEnumerable<RecordedEvent>> ReadEventsBatch(string stream, long fromNumber, int count, bool resolveLinkTos, Func<ResolvedEvent, Task> eventHandlerCallback = null)
        {
            var readCorrelationId = Guid.NewGuid();
            var pendingReadTask = new TaskCompletionSource<ReadStreamEventsCompleted>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (_pendingReads.TryAdd(readCorrelationId, pendingReadTask))
            {
                var readMessage = new ReadStreamEvents()
                {
                    EventStreamId = stream,
                    FromEventNumber = fromNumber,
                    MaxCount = count,
                    ResolveLinkTos = resolveLinkTos,
                    RequireMaster = false
                };

                ArraySegment<byte> requestSerialized;
                using (var memory = new MemoryStream())
                {
                    readMessage.WriteTo(memory);
                    requestSerialized = new ArraySegment<byte>(memory.GetBuffer(), 0, (int)memory.Length);
                }

                var package = new TcpPackage(TcpCommand.ReadStreamEventsForward, readCorrelationId, requestSerialized.ToArray<byte>());
                _pendingSendMessages.Enqueue(package);

                // Wait for read to complete
                var result = await pendingReadTask.Task.ConfigureAwait(false);
                _pendingReads.TryRemove(readCorrelationId, out var _);

                // Return result
                var events = new List<RecordedEvent>();
                foreach (var e in result.Events)
                {
                    // Create result
                    var recordedEvent = new RecordedEvent()
                    {
                        Stream = e.Event.EventStreamId,
                        Id = new Guid(e.Event.EventId.ToByteArray()),
                        Created = new DateTime(e.Event.Created),
                        Data = new ArraySegment<byte>(e.Event.Data.ToByteArray()),
                        MetaData = new ArraySegment<byte>(e.Event.Metadata.ToByteArray()),
                        EventNumber = e.Event.EventNumber,
                        EventType = e.Event.EventType,
                        IsJson = e.Event.DataContentType == 1
                    };
                    events.Add(recordedEvent);

                    // Event Handler
                    var resolvedEvent = new ResolvedEvent()
                    {
                        Event = e.Event,
                        Link = e.Link,
                        // TODO: Determine if this is correct
                        CommitPosition = e.Event.EventNumber,
                        PreparePosition = e.Event.EventNumber
                    };

                    if (eventHandlerCallback != null) await eventHandlerCallback(resolvedEvent).ConfigureAwait(false);
                }

                return events;
            }
            throw new Exception("CorrelationId already in use?");
        }
    }
}
