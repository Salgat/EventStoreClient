using EventStoreClient.Connection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventStoreClient.Test
{
    [TestClass]
    public class ConnectionTest
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
            var events = new List<CreateEvent>()
            {
                new CreateEvent()
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
            var events = new List<CreateEvent>()
            {
                new CreateEvent()
                {
                    Id = Guid.NewGuid(),
                    EventType = "TestType",
                    IsJson = true,
                    Data = Encoding.UTF8.GetBytes("{}"),
                    MetaData = Encoding.UTF8.GetBytes("{}")
                },
                new CreateEvent()
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
            var events = new List<CreateEvent>()
            {
                new CreateEvent()
                {
                    Id = Guid.NewGuid(),
                    EventType = "TestType",
                    IsJson = true,
                    Data = Encoding.UTF8.GetBytes("{}"),
                    MetaData = Encoding.UTF8.GetBytes("{}")
                }
            };
            var events2 = new List<CreateEvent>()
            {
                new CreateEvent()
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
            var events = new List<CreateEvent>()
            {
                new CreateEvent()
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

        #region Read Events
        [TestMethod]
        public async Task ReadEvent_Success()
        {
            var credentials = new UserCredentials("admin", "changeit");
            var connectionSettings = new ConnectionSettings(credentials, "127.0.0.1", 1113, "myConnection");
            var connection = new EventStoreConnection(connectionSettings);

            await connection.ConnectAsync().ConfigureAwait(false);

            var streamId = $"TestStream-{Guid.NewGuid()}";
            var events = new List<CreateEvent>()
            {
                new CreateEvent()
                {
                    Id = Guid.NewGuid(),
                    EventType = "TestType",
                    IsJson = true,
                    Data = Encoding.UTF8.GetBytes("{\"Key\": \"Value\"}"),
                    MetaData = Encoding.UTF8.GetBytes("{}")
                }
            };

            await connection.WriteEvents(events, streamId, -1).ConfigureAwait(false);

            var readEvents = await connection.ReadEvents(streamId, 0, 4095, true).ConfigureAwait(false);
            var readEventsList = readEvents.ToList();

            Assert.IsNotNull(readEvents);
            Assert.AreEqual(events.Count, readEventsList.Count);
            Assert.AreEqual(events[0].Id, readEventsList[0].Id);
            Assert.AreEqual(streamId, readEventsList[0].Stream);
            Assert.AreEqual(events[0].IsJson, readEventsList[0].IsJson);
            Assert.AreEqual(events[0].Data.ToString(), readEventsList[0].Data.ToString());
            Assert.AreEqual(events[0].MetaData.ToString(), readEventsList[0].MetaData.ToString());
        }
        #endregion
    }
}
