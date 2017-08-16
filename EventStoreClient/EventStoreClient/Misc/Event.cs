using System;
using System.Collections.Generic;
using System.Text;

namespace EventStoreClient
{
    public class Event
    {
        public Guid Id { get; set; }
        public string EventType { get; set; }
        public bool IsJson { get; set; }
        public ArraySegment<byte> Data { get; set; }
        public ArraySegment<byte> MetaData { get; set; }
    }
}
