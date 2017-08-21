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
        /// Write events to specific stream.
        /// </summary>
        /// <param name="events"></param>
        /// <param name="stream"></param>
        /// <param name="expectedVersion"></param>
        /// <returns></returns>
        public async Task WriteEvents(IEnumerable<CreateEvent> events, string stream, long expectedVersion)
        {
            var writeCorrelationId = Guid.NewGuid();
            var pendingWriteTask = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (_pendingWrites.TryAdd(writeCorrelationId, pendingWriteTask))
            {
                // Populate protobuf objects for events
                var eventsToWrite = events.Select(e => new NewEvent()
                {
                    EventId = ByteString.CopyFrom(e.Id.ToByteArray()),
                    EventType = e.EventType,
                    Data = ByteString.CopyFrom(e.Data.Array),
                    DataContentType = e.IsJson ? 1 : 0,
                    Metadata = ByteString.CopyFrom(e.MetaData.Array),
                    MetadataContentType = 0
                });

                var writeEventsMessage = new WriteEvents()
                {
                    EventStreamId = stream,
                    ExpectedVersion = expectedVersion,
                    RequireMaster = true
                };
                writeEventsMessage.Events.AddRange(eventsToWrite);

                // Serialize events
                ArraySegment<byte> eventsSerialized;
                using (var memory = new MemoryStream())
                {
                    writeEventsMessage.WriteTo(memory);
                    eventsSerialized = new ArraySegment<byte>(memory.GetBuffer(), 0, (int)memory.Length);
                }

                // Send write package
                var package = new TcpPackage(TcpCommand.WriteEvents, writeCorrelationId, eventsSerialized.ToArray<byte>());
                _pendingSendMessages.Enqueue(package);

                // Wait for write to be acknowledged by server then remove completed task
                await pendingWriteTask.Task.ConfigureAwait(false);
                _pendingWrites.TryRemove(writeCorrelationId, out var _);

                return;
            }
            throw new Exception("CorrelationId already in use?");
        }
    }
}
