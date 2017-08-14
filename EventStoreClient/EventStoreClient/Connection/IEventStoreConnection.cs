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
        
        ConnectionSettings Settings { get; }
    }
}
