using EventStoreClient.Connection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace EventStoreClient.Test
{
    [TestClass]
    public class Connection
    {
        [TestMethod]
        public async Task Connection_Success()
        {
            var credentials = new UserCredentials("admin", "changeit");
            var connectionSettings = new ConnectionSettings(credentials, "127.0.0.1", 1113, "myConnection");
            var connection = new EventStoreConnection(connectionSettings);

            // TODO: This could fail and we couldn't know any better
            await connection.ConnectAsync().ConfigureAwait(false);
        }

        #region Write Events
        [TestMethod]
        public async Task WriteEvent_Success()
        {
            var credentials = new UserCredentials("admin", "changeit");
            var connectionSettings = new ConnectionSettings(credentials, "127.0.0.1", 1113, "myConnection");
            var connection = new EventStoreConnection(connectionSettings);

            await connection.ConnectAsync().ConfigureAwait(false);

            var streamId = $"TestStream-{Guid.NewGuid()}";
            var events = new List<Event>()
            {
                new Event()
                {
                    Id = Guid.NewGuid(),
                    EventType = "TestType",
                    IsJson = true,
                    Data = Encoding.UTF8.GetBytes("{}"),
                    MetaData = Encoding.UTF8.GetBytes("{}")
                }
            };

            await connection.WriteEvents(events, streamId, -1).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task WriteMultipleEvents_SingleWriteOperation_Success()
        {
            var credentials = new UserCredentials("admin", "changeit");
            var connectionSettings = new ConnectionSettings(credentials, "127.0.0.1", 1113, "myConnection");
            var connection = new EventStoreConnection(connectionSettings);

            await connection.ConnectAsync().ConfigureAwait(false);

            var streamId = $"TestStream-{Guid.NewGuid()}";
            var events = new List<Event>()
            {
                new Event()
                {
                    Id = Guid.NewGuid(),
                    EventType = "TestType",
                    IsJson = true,
                    Data = Encoding.UTF8.GetBytes("{}"),
                    MetaData = Encoding.UTF8.GetBytes("{}")
                },
                new Event()
                {
                    Id = Guid.NewGuid(),
                    EventType = "TestType",
                    IsJson = true,
                    Data = Encoding.UTF8.GetBytes("{}"),
                    MetaData = Encoding.UTF8.GetBytes("{}")
                },
            };

            await connection.WriteEvents(events, streamId, -1).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task WriteMultipleEvents_MultipleWriteOperations_Success()
        {
            var credentials = new UserCredentials("admin", "changeit");
            var connectionSettings = new ConnectionSettings(credentials, "127.0.0.1", 1113, "myConnection");
            var connection = new EventStoreConnection(connectionSettings);

            await connection.ConnectAsync().ConfigureAwait(false);

            var streamId = $"TestStream-{Guid.NewGuid()}";
            var events = new List<Event>()
            {
                new Event()
                {
                    Id = Guid.NewGuid(),
                    EventType = "TestType",
                    IsJson = true,
                    Data = Encoding.UTF8.GetBytes("{}"),
                    MetaData = Encoding.UTF8.GetBytes("{}")
                }
            };
            var events2 = new List<Event>()
            {
                new Event()
                {
                    Id = Guid.NewGuid(),
                    EventType = "TestType",
                    IsJson = true,
                    Data = Encoding.UTF8.GetBytes("{}"),
                    MetaData = Encoding.UTF8.GetBytes("{}")
                }
            };

            await connection.WriteEvents(events, streamId, -1).ConfigureAwait(false);
            await connection.WriteEvents(events2, streamId, 0).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task WriteEvent_WrongExpectedVersion()
        {
            var credentials = new UserCredentials("admin", "changeit");
            var connectionSettings = new ConnectionSettings(credentials, "127.0.0.1", 1113, "myConnection");
            var connection = new EventStoreConnection(connectionSettings);

            await connection.ConnectAsync().ConfigureAwait(false);

            var streamId = $"TestStream-{Guid.NewGuid()}";
            var events = new List<Event>()
            {
                new Event()
                {
                    Id = Guid.NewGuid(),
                    EventType = "TestType",
                    IsJson = true,
                    Data = Encoding.UTF8.GetBytes("{}"),
                    MetaData = Encoding.UTF8.GetBytes("{}")
                }
            };

            Exception exception = null;
            try
            {
                await connection.WriteEvents(events, streamId, 0).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            Assert.IsNotNull(exception);
        }
        #endregion
    }
}
