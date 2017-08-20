using EventStoreClient.Connection;
using EventStoreClient.Misc;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
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

            var streamId = $"TestStream-{Guid.NewGuid():N}";
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

            var streamId = $"TestStream-{Guid.NewGuid():N}";
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
        public async Task WriteMultipleEvents_SingleLargeWriteOperation_Success()
        {
            var credentials = new UserCredentials("admin", "changeit");
            var connectionSettings = new ConnectionSettings(credentials, "127.0.0.1", 1113, "myConnection");
            var connection = new EventStoreConnection(connectionSettings);

            await connection.ConnectAsync().ConfigureAwait(false);

            var streamId = $"TestStream-{Guid.NewGuid():N}";

            var events = new List<CreateEvent>();
            for (var i = 0; i < 100000; ++i)
            {
                var eventToWrite = new CreateEvent()
                {
                    Id = Guid.NewGuid(),
                    EventType = "TestType",
                    IsJson = true,
                    Data = Encoding.UTF8.GetBytes("{}"),
                    MetaData = Encoding.UTF8.GetBytes("{}")
                };
                events.Add(eventToWrite);
            }
            
            await connection.WriteEvents(events, streamId, -1).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task WriteMultipleEvents_MultipleWriteOperations_Success()
        {
            var credentials = new UserCredentials("admin", "changeit");
            var connectionSettings = new ConnectionSettings(credentials, "127.0.0.1", 1113, "myConnection");
            var connection = new EventStoreConnection(connectionSettings);

            await connection.ConnectAsync().ConfigureAwait(false);

            var streamId = $"TestStream-{Guid.NewGuid():N}";
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

            var streamId = $"TestStream-{Guid.NewGuid():N}";
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

            var streamId = $"TestStream-{Guid.NewGuid():N}";
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

        #region Catchup Subscription
        [TestMethod]
        public async Task CatchupSubscription_ReceivesEvents_Success()
        {
            var streamId = $"TestStream-{Guid.NewGuid():N}";
            var credentials = new UserCredentials("admin", "changeit");
            var connectionSettings = new ConnectionSettings(credentials, "127.0.0.1", 1113, "myConnection");
            var connection = new EventStoreConnection(connectionSettings);
            await connection.ConnectAsync().ConfigureAwait(false);

            // First write a few events before the subscription
            const int initialEventCounter = 3;
            for (var i = 0; i < initialEventCounter; ++i)
            {
                await WriteRandomEventToStream(streamId, connection).ConfigureAwait(false);
            }
            
            // Start catchup subscription
            var totalEventsHandled = 0;
            var subscription = await connection.CreateCatchupSubscription(streamId, 0, async e =>
            {
                Interlocked.Increment(ref totalEventsHandled);
            }).ConfigureAwait(false);


            const int afterEventCounter = 4;
            for (var i = 0; i < afterEventCounter; ++i)
            {
                await WriteRandomEventToStream(streamId, connection).ConfigureAwait(false);
            }

            const int attempts = 5;
            for (var i = 0; i < attempts; ++i)
            {
                if (Thread.VolatileRead(ref totalEventsHandled) == initialEventCounter + afterEventCounter)
                {
                    return;
                }
                await Task.Delay(500).ConfigureAwait(false);
            }
            throw new Exception($"Only {Thread.VolatileRead(ref totalEventsHandled)} events handled of expected {initialEventCounter + afterEventCounter}");
        }


        private async Task WriteRandomEventToStream(string stream, IEventStoreConnection connection)
        {
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
            await connection.WriteEvents(events, stream, ExpectedEventNumber.Any).ConfigureAwait(false);
        }

        #endregion
    }
}
