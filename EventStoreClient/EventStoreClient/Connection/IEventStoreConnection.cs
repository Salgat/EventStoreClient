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

        Task WriteEvents(IEnumerable<Event> events, string stream, long expectedEventNumber);
        
        ConnectionSettings Settings { get; }
    }
}
