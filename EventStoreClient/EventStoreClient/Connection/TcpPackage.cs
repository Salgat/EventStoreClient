using System;
using System.Collections.Generic;
using System.Text;

namespace EventStoreClient.Connection
{
    internal enum TcpCommand : byte
    {
        HeartbeatRequestCommand = 0x01,
        HeartbeatResponseCommand = 0x02,

        Ping = 0x03,
        Pong = 0x04,

        PrepareAck = 0x05,
        CommitAck = 0x06,

        SlaveAssignment = 0x07,
        CloneAssignment = 0x08,

        SubscribeReplica = 0x10,
        ReplicaLogPositionAck = 0x11,
        CreateChunk = 0x12,
        RawChunkBulk = 0x13,
        DataChunkBulk = 0x14,
        ReplicaSubscriptionRetry = 0x15,
        ReplicaSubscribed = 0x16,

        // CLIENT COMMANDS
        //        CreateStream = 0x80,
        //        CreateStreamCompleted = 0x81,

        WriteEvents = 0x82,
        WriteEventsCompleted = 0x83,

        TransactionStart = 0x84,
        TransactionStartCompleted = 0x85,
        TransactionWrite = 0x86,
        TransactionWriteCompleted = 0x87,
        TransactionCommit = 0x88,
        TransactionCommitCompleted = 0x89,

        DeleteStream = 0x8A,
        DeleteStreamCompleted = 0x8B,

        ReadEvent = 0xB0,
        ReadEventCompleted = 0xB1,
        ReadStreamEventsForward = 0xB2,
        ReadStreamEventsForwardCompleted = 0xB3,
        ReadStreamEventsBackward = 0xB4,
        ReadStreamEventsBackwardCompleted = 0xB5,
        ReadAllEventsForward = 0xB6,
        ReadAllEventsForwardCompleted = 0xB7,
        ReadAllEventsBackward = 0xB8,
        ReadAllEventsBackwardCompleted = 0xB9,

        SubscribeToStream = 0xC0,
        SubscriptionConfirmation = 0xC1,
        StreamEventAppeared = 0xC2,
        UnsubscribeFromStream = 0xC3,
        SubscriptionDropped = 0xC4,
        ConnectToPersistentSubscription = 0xC5,
        PersistentSubscriptionConfirmation = 0xC6,
        PersistentSubscriptionStreamEventAppeared = 0xC7,
        CreatePersistentSubscription = 0xC8,
        CreatePersistentSubscriptionCompleted = 0xC9,
        DeletePersistentSubscription = 0xCA,
        DeletePersistentSubscriptionCompleted = 0xCB,
        PersistentSubscriptionAckEvents = 0xCC,
        PersistentSubscriptionNakEvents = 0xCD,
        UpdatePersistentSubscription = 0xCE,
        UpdatePersistentSubscriptionCompleted = 0xCF,

        ScavengeDatabase = 0xD0,
        ScavengeDatabaseCompleted = 0xD1,

        BadRequest = 0xF0,
        NotHandled = 0xF1,
        Authenticate = 0xF2,
        Authenticated = 0xF3,
        NotAuthenticated = 0xF4,
        IdentifyClient = 0xF5,
        ClientIdentified = 0xF6,
    }

    [Flags]
    internal enum TcpFlags : byte
    {
        None = 0x00,
        Authenticated = 0x01,
    }

    internal struct TcpPackage
    {
        public const int CommandOffset = 0;
        public const int FlagsOffset = CommandOffset + 1;
        public const int CorrelationOffset = FlagsOffset + 1;
        public const int AuthOffset = CorrelationOffset + 16;

        public const int MandatorySize = AuthOffset;

        public readonly TcpCommand Command;
        public readonly TcpFlags Flags;
        public readonly Guid CorrelationId;
        public readonly string Login;
        public readonly string Password;
        public readonly ArraySegment<byte> Data;
        
        public static readonly UTF8Encoding UTF8NoBom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

        public static TcpPackage FromArraySegment(ArraySegment<byte> data)
        {
            if (data.Count < MandatorySize)
                throw new ArgumentException(string.Format("ArraySegment too short, length: {0}", data.Count), "data");

            var command = (TcpCommand)data.Array[data.Offset + CommandOffset];
            var flags = (TcpFlags)data.Array[data.Offset + FlagsOffset];

            var guidBytes = new byte[16];
            Buffer.BlockCopy(data.Array, data.Offset + CorrelationOffset, guidBytes, 0, 16);
            var correlationId = new Guid(guidBytes);

            var headerSize = MandatorySize;
            string login = null;
            string pass = null;
            if ((flags & TcpFlags.Authenticated) != 0)
            {
                var loginLen = data.Array[data.Offset + AuthOffset];
                if (AuthOffset + 1 + loginLen + 1 >= data.Count)
                    throw new Exception("Login length is too big, it does not fit into TcpPackage.");
                login = UTF8NoBom.GetString(data.Array, data.Offset + AuthOffset + 1, loginLen);

                var passLen = data.Array[data.Offset + AuthOffset + 1 + loginLen];
                if (AuthOffset + 1 + loginLen + 1 + passLen > data.Count)
                    throw new Exception("Password length is too big, it does not fit into TcpPackage.");
                pass = UTF8NoBom.GetString(data.Array, data.Offset + AuthOffset + 1 + loginLen + 1, passLen);

                headerSize += 1 + loginLen + 1 + passLen;
            }

            return new TcpPackage(command,
                                  flags,
                                  correlationId,
                                  login,
                                  pass,
                                  new ArraySegment<byte>(data.Array, data.Offset + headerSize, data.Count - headerSize));
        }

        public TcpPackage(TcpCommand command, TcpFlags flags, Guid correlationId, string login, string password, ArraySegment<byte> data)
        {
            Command = command;
            Flags = flags;
            CorrelationId = correlationId;
            Login = login;
            Password = password;
            Data = data;
        }

        public TcpPackage(TcpCommand command, Guid correlationId, byte[] data)
            : this(command, TcpFlags.None, correlationId, null, null, data)
        {
        }

        public TcpPackage(TcpCommand command, TcpFlags flags, Guid correlationId, string login, string password, byte[] data)
            : this(command, flags, correlationId, login, password, new ArraySegment<byte>(data ?? new byte[0]))
        {
        }

        public byte[] AsByteArray()
        {
            if ((Flags & TcpFlags.Authenticated) != 0)
            {
                var loginLen = UTF8NoBom.GetByteCount(Login);
                var passLen = UTF8NoBom.GetByteCount(Password);
                if (loginLen > 255) throw new ArgumentException(string.Format("Login serialized length should be less than 256 bytes (but is {0}).", loginLen));
                if (passLen > 255) throw new ArgumentException(string.Format("Password serialized length should be less than 256 bytes (but is {0}).", passLen));

                var res = new byte[MandatorySize + 2 + loginLen + passLen + Data.Count];
                res[CommandOffset] = (byte)Command;
                res[FlagsOffset] = (byte)Flags;
                Buffer.BlockCopy(CorrelationId.ToByteArray(), 0, res, CorrelationOffset, 16);

                res[AuthOffset] = (byte)loginLen;
                UTF8NoBom.GetBytes(Login, 0, Login.Length, res, AuthOffset + 1);
                res[AuthOffset + 1 + loginLen] = (byte)passLen;
                UTF8NoBom.GetBytes(Password, 0, Password.Length, res, AuthOffset + 1 + loginLen + 1);

                Buffer.BlockCopy(Data.Array, Data.Offset, res, res.Length - Data.Count, Data.Count);
                return res;
            }
            else
            {
                var res = new byte[MandatorySize + Data.Count];
                res[CommandOffset] = (byte)Command;
                res[FlagsOffset] = (byte)Flags;
                Buffer.BlockCopy(CorrelationId.ToByteArray(), 0, res, CorrelationOffset, 16);
                Buffer.BlockCopy(Data.Array, Data.Offset, res, res.Length - Data.Count, Data.Count);
                return res;
            }
        }

        public ArraySegment<byte> AsArraySegment()
        {
            return new ArraySegment<byte>(AsByteArray());
        }
    }

}
