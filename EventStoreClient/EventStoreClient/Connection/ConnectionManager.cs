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
using System.Threading;
using static EventStore.Client.Messages.ReadStreamEventsCompleted.Types;

namespace EventStoreClient.Connection
{
    internal partial class ConnectionManager
    {
        private readonly ConnectionSettings _settings;
        private bool _connected = false;
        private Socket _connection = null;
        private SemaphoreSlim _connectionLock = new SemaphoreSlim(1);
        readonly ConcurrentQueue<TcpPackage> _pendingMessages = new ConcurrentQueue<TcpPackage>();
        readonly ConcurrentQueue<TcpPackage> _pendingSendMessages = new ConcurrentQueue<TcpPackage>();
        private readonly ArraySegment<byte> _receiveBuffer = new ArraySegment<byte>(new byte[TcpConfiguration.SocketBufferSize]);

        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<object>> _pendingWrites = new ConcurrentDictionary<Guid, TaskCompletionSource<object>>();
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource<ReadStreamEventsCompleted>> _pendingReads = new ConcurrentDictionary<Guid, TaskCompletionSource<ReadStreamEventsCompleted>>();
        private readonly ConcurrentDictionary<Guid, CatchupSubscription> _catchupSubscriptions = new ConcurrentDictionary<Guid, CatchupSubscription>();

        public ConnectionManager(ConnectionSettings settings)
        {
            _settings = settings;
        }

        public async Task StartConnection()
        {
            await _connectionLock.WaitAsync().ConfigureAwait(false);
            try
            {
                var ip = IPAddress.Parse(_settings.HostAddress);
                var remoteEndpoint = new IPEndPoint(ip, _settings.Port);
                
                _connection = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await _connection.ConnectAsync(remoteEndpoint).ConfigureAwait(false);
                _connected = true;
            }
            finally
            {
                _connectionLock.Release();
            }

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

        public async Task CloseConnectionAsync()
        {
            await _connectionLock.WaitAsync().ConfigureAwait(false);
            try
            {
                _connected = false;
                _connection.Shutdown(SocketShutdown.Both);
                _connection.Dispose();
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        /// <summary>
        /// Receives and sends any pending messages.
        /// </summary>
        /// <returns></returns>
        private async Task ManageConnection()
        {
            var incomingMessagesTask = HandleIncomingMessagesAsync();
            var handlePendingSendMessagesTask = HandlePendingSendMessages();
            var handlePendingCatchupEventsTask = _catchupSubscriptions.Select(subscription => subscription.Value.HandlePendingEvents());

            var pendingTasks = new List<Task>()
            {
                incomingMessagesTask,
                handlePendingSendMessagesTask
            };
            pendingTasks.AddRange(handlePendingCatchupEventsTask);

            await Task.WhenAll(pendingTasks).ConfigureAwait(false);
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

            await _connectionLock.WaitAsync().ConfigureAwait(false);
            try
            {
                await _connection.SendAsync(framed, SocketFlags.None).ConfigureAwait(false);
            }
            finally
            {
                _connectionLock.Release();
            }
        }

        private async Task HandleIncomingMessagesAsync()
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

                // Catchup Subscription started
                if (package.Command == TcpCommand.SubscriptionConfirmation)
                {
                    if (_catchupSubscriptions.TryGetValue(package.CorrelationId, out var catchupSubscription))
                    {
                        // Acknowledge subscription live
                        var response = SubscriptionConfirmation.Parser.ParseFrom(package.Data.ToArray<byte>());
                        catchupSubscription.SubscriptionStarted.SetResult(response.LastEventNumber);
                    }
                    return;
                }

                // Catchup Subscription dropped
                if (package.Command == TcpCommand.SubscriptionDropped)
                {
                    if (_catchupSubscriptions.TryGetValue(package.CorrelationId, out var catchupSubscription))
                    {
                        // Close subscription
                        await catchupSubscription.CloseSubscriptionAsync().ConfigureAwait(false);
                    }
                    return;
                }

                // Catchup Subscription event appeared
                if (package.Command == TcpCommand.StreamEventAppeared)
                {
                    if (_catchupSubscriptions.TryGetValue(package.CorrelationId, out var catchupSubscription))
                    {
                        // Handle event
                        var eventAppeared = StreamEventAppeared.Parser.ParseFrom(package.Data.ToArray<byte>());
                        catchupSubscription.AddEvent(eventAppeared.Event);
                    }
                    return;
                }
            }
        }
    }
}
