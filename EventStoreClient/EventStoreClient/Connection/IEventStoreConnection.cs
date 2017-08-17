using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace EventStoreClient
{
    public interface IEventStoreConnection
    {
        string ConnectionName { get; }
        Task ConnectAsync();
        Task CloseAsync();

        Task WriteEvents(IEnumerable<CreateEvent> events, string stream, long expectedEventNumber);
        Task<IEnumerable<RecordedEvent>> ReadEvents(string stream, long fromNumber, int count, bool resolveLinkTos);


        ConnectionSettings Settings { get; }
    }
}
