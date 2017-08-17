using System;

namespace EventStoreClient
{
    public class CreateEvent
    {
        public Guid Id { get; set; }
        public string EventType { get; set; }
        public bool IsJson { get; set; }
        public ArraySegment<byte> Data { get; set; }
        public ArraySegment<byte> MetaData { get; set; }
    }

    public class RecordedEvent
    {
        // TODO: Should probably have this read-only
        public string Stream { get; set; }
        public Guid Id { get; set; }
        public string EventType { get; set; }
        public ArraySegment<byte> Data { get; set; }
        public ArraySegment<byte> MetaData { get; set; }
        public DateTime Created { get; set; }
        public long EventNumber { get; set; }
        public bool IsJson { get; set; }
    }
}
