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
        private bool connected = false;
        private Socket connection = null;
        LengthPrefixMessageFramer _framer;
        ConcurrentQueue<TcpPackage> _pendingMessages = new ConcurrentQueue<TcpPackage>();
        ConcurrentQueue<TcpPackage> _pendingSendMessages = new ConcurrentQueue<TcpPackage>();
        private readonly ArraySegment<byte> _receiveBuffer = new ArraySegment<byte>(new byte[TcpConfiguration.SocketBufferSize]);

        public ConnectionManager(ConnectionSettings settings)
        {
            _settings = settings;
            _framer = new LengthPrefixMessageFramer();
            _framer.RegisterMessageArrivedCallback(IncomingMessageArrived);
            
            // Initialize for first time
        }

        public async Task StartConnection()
        {
            var ip = IPAddress.Parse(_settings.HostAddress);
            var remoteEndpoint = new IPEndPoint(ip, _settings.Port);

            connection = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await connection.ConnectAsync(remoteEndpoint).ConfigureAwait(false);
            connected = true;

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            Task.Run(async () =>
            {
                Listen();
                while (connected)
                {
                    await ManageConnection().ConfigureAwait(false);
                    await Task.Delay(10).ConfigureAwait(false);
                }
            });
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
        }

        public async Task CloseConnectionAsync()
        {
            connected = false;
            connection.Shutdown(SocketShutdown.Both);
            connection.Dispose();
        }

        /// <summary>
        /// Receives and sends any pending messages.
        /// </summary>
        /// <returns></returns>
        private async Task ManageConnection()
        {
            await HandleIncomingMessages().ConfigureAwait(false);
            await HandlePendingSendMessages().ConfigureAwait(false);
        }

        private async Task Listen()
        {
            while (connected)
            {
                int size = 0;
                try
                {
                    size = await connection.ReceiveAsync(_receiveBuffer, SocketFlags.None).ConfigureAwait(false);
                }
                catch (SocketException ex)
                {
                    if (ex.SocketErrorCode == SocketError.NotConnected)
                    {
                        connected = false;
                    }
                    return;
                }
                var data = new ArraySegment<byte>(_receiveBuffer.Array, 0, size);
                _framer.UnFrameData(data);
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

        private async Task HandleIncomingMessages()
        {
            while (_pendingMessages.TryDequeue(out TcpPackage package))
            {
                // Ignore messages if connection lost
                if (connected == false) return;

                // Response ignored
                if (package.Command == TcpCommand.HeartbeatResponseCommand) return;

                // Heartbeat requested
                if (package.Command == TcpCommand.HeartbeatRequestCommand)
                {
                    var heartbeatMessage = new TcpPackage(TcpCommand.HeartbeatResponseCommand, package.CorrelationId, null);
                    _pendingSendMessages.Enqueue(heartbeatMessage);
                    return;
                }
            }
        }

        private async Task SendMessage(TcpPackage package)
        {
            var data = package.AsArraySegment();
            var framed = _framer.FrameData(data);
            await connection.SendAsync(framed, SocketFlags.None).ConfigureAwait(false);
        }
    }
}
