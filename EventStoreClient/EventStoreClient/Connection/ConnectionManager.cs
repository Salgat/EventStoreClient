using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using Google.Protobuf;
using System.Collections.Concurrent;
using System.Linq;
using System.Diagnostics;
using EventStore.Client.Messages;
using Google.Protobuf.Collections;
using System.IO;
using static EventStore.Client.Messages.ReadStreamEventsCompleted.Types;

namespace EventStoreClient.Connection
{
    internal static class TcpConfiguration
    {
        public const int SocketCloseTimeoutMs = 500;

        public const int AcceptBacklogCount = 1000;
        public const int ConcurrentAccepts = 1;
        public const int AcceptPoolSize = ConcurrentAccepts * 2;

        public const int ConnectPoolSize = 32;
        public const int SendReceivePoolSize = 512;

        public const int BufferChunksCount = 512;
        public const int SocketBufferSize = 8 * 1024;
    }

    internal class ConnectionManager
    {
        private readonly ConnectionSettings _settings;
        private bool _connected = false;
        private Socket _connection = null;
        readonly ConcurrentQueue<TcpPackage> _pendingMessages = new ConcurrentQueue<TcpPackage>();
        readonly ConcurrentQueue<TcpPackage> _pendingSendMessages = new ConcurrentQueue<TcpPackage>();
        private readonly ArraySegment<byte> _receiveBuffer = new ArraySegment<byte>(new byte[TcpConfiguration.SocketBufferSize]);

        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<object>> _pendingWrites = new ConcurrentDictionary<Guid, TaskCompletionSource<object>>();
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<ReadStreamEventsCompleted>> _pendingReads = new ConcurrentDictionary<Guid, TaskCompletionSource<ReadStreamEventsCompleted>>();

        public ConnectionManager(ConnectionSettings settings)
        {
            _settings = settings;
        }

        public async Task StartConnection()
        {
            var ip = IPAddress.Parse(_settings.HostAddress);
            var remoteEndpoint = new IPEndPoint(ip, _settings.Port);

            _connection = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await _connection.ConnectAsync(remoteEndpoint).ConfigureAwait(false);
            _connected = true;

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            Task.Run(async () =>
            {
                Listen();
                while (_connected)
                {
                    await ManageConnection().ConfigureAwait(false);
                    await Task.Delay(10).ConfigureAwait(false);
                }
            });
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        }

        public void CloseConnection()
        {
            _connected = false;
            _connection.Shutdown(SocketShutdown.Both);
            _connection.Dispose();
        }

        /// <summary>
        /// Receives and sends any pending messages.
        /// </summary>
        /// <returns></returns>
        private async Task ManageConnection()
        {
            HandleIncomingMessages();
            await HandlePendingSendMessages().ConfigureAwait(false);
        }

        private async Task Listen()
        {
            var framer = new LengthPrefixMessageFramer(IncomingMessageArrived);
            while (_connected)
            {
                var size = 0;
                try
                {
                    size = await _connection.ReceiveAsync(_receiveBuffer, SocketFlags.None).ConfigureAwait(false);
                }
                catch (SocketException ex)
                {
                    if (ex.SocketErrorCode == SocketError.NotConnected)
                    {
                        _connected = false;
                    }
                    return;
                }
                var data = new ArraySegment<byte>(_receiveBuffer.Array, 0, size);
                framer.UnFrameData(data);
            }
        }

        private async Task HandlePendingSendMessages()
        {
            var sendsInProgress = new List<Task>();
            while (_pendingSendMessages.TryDequeue(out TcpPackage package))
            {
                sendsInProgress.Add(SendMessage(package));
            }
            await Task.WhenAll(sendsInProgress).ConfigureAwait(false);
        }

        private void IncomingMessageArrived(ArraySegment<byte> data)
        {
            var package = TcpPackage.FromArraySegment(data);
            _pendingMessages.Enqueue(package);
        }

        private async Task SendMessage(TcpPackage package)
        {
            var data = package.AsArraySegment();
            var framed = LengthPrefixMessageFramer.FrameData(data);
            await _connection.SendAsync(framed, SocketFlags.None).ConfigureAwait(false);
        }

        private void HandleIncomingMessages()
        {
            while (_pendingMessages.TryDequeue(out TcpPackage package))
            {
                // Ignore messages if connection lost
                if (_connected == false) return;

                // Response ignored
                if (package.Command == TcpCommand.HeartbeatResponseCommand) return;

                // Heartbeat requested
                if (package.Command == TcpCommand.HeartbeatRequestCommand)
                {
                    var heartbeatMessage = new TcpPackage(TcpCommand.HeartbeatResponseCommand, package.CorrelationId, null);
                    _pendingSendMessages.Enqueue(heartbeatMessage);
                    return;
                }

                // Write events completed
                if (package.Command == TcpCommand.WriteEventsCompleted)
                {
                    if (_pendingWrites.TryGetValue(package.CorrelationId, out TaskCompletionSource<object> pendingWrite))
                    {
                        if (pendingWrite != null)
                        {
                            var response = WriteEventsCompleted.Parser.ParseFrom(package.Data.ToArray<byte>());

                            switch (response.Result)
                            {
                                // TODO: Implement proper exception classes
                                case OperationResult.Success:
                                    pendingWrite.SetResult(new object());
                                    break;
                                case OperationResult.CommitTimeout:
                                    pendingWrite.SetException(new Exception("CommitTimeout exception"));
                                    break;
                                case OperationResult.WrongExpectedVersion:
                                    pendingWrite.SetException(new Exception("WrongExpectedVersion exception"));
                                    break;
                                default:
                                    pendingWrite.SetException(new Exception($"Unexpected exception: {response.Message}"));
                                    break;
                            }
                        }
                    }
                    return;
                }

                // Read events completed
                if (package.Command == TcpCommand.ReadStreamEventsForwardCompleted)
                {
                    if (_pendingReads.TryGetValue(package.CorrelationId, out TaskCompletionSource<ReadStreamEventsCompleted> pendingRead))
                    {
                        if (pendingRead != null)
                        {
                            var response = ReadStreamEventsCompleted.Parser.ParseFrom(package.Data.ToArray<byte>());

                            switch (response.Result)
                            {
                                case ReadStreamResult.Success:
                                case ReadStreamResult.NoStream:
                                    pendingRead.SetResult(response);
                                    break;
                                default:
                                    pendingRead.SetException(new Exception($"Unexpected exception"));
                                    break;
                            }
                        }
                    }
                    return;
                }
            }
        }

        public async Task<IEnumerable<RecordedEvent>> ReadEvents(string stream, long fromNumber, int count, bool resolveLinkTos)
        {
            const int batchSize = 512;
            var result = new List<RecordedEvent>();
            int currentFromNumber = 0;
            long lastEventNumber = fromNumber + count - 1;
            while (currentFromNumber < lastEventNumber)
            {
                // TODO: Update batch size to be configurable
                var batchResult = (await ReadEventsBatch(stream, currentFromNumber, batchSize, resolveLinkTos).ConfigureAwait(false)).ToList();
                result.AddRange(batchResult);

                // If this read resulted in no events, we are done reading
                if (!batchResult.Any()) break;

                // Continue to next batch of events
                currentFromNumber += batchSize;
            }

            return result;
        }

        private async Task<IEnumerable<RecordedEvent>> ReadEventsBatch(string stream, long fromNumber, int count, bool resolveLinkTos)
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
                return result.Events.Select(e => new RecordedEvent()
                {
                    Stream = e.Event.EventStreamId,
                    Id = new Guid(e.Event.EventId.ToByteArray()),
                    Created = new DateTime(e.Event.Created),
                    Data = new ArraySegment<byte>(e.Event.Data.ToByteArray()),
                    MetaData = new ArraySegment<byte>(e.Event.Metadata.ToByteArray()),
                    EventNumber = e.Event.EventNumber,
                    EventType = e.Event.EventType,
                    IsJson = e.Event.DataContentType == 1
                });
            }
            throw new Exception("CorrelationId already in use?");
        }

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
                var eventsToWrite = events.Select(e => new NewEvent() {
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
