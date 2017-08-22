using System;
using System.Collections.Generic;
using System.Text;

namespace EventStoreClient.Exceptions
{
    internal class ReconnectionException : Exception
    {
        public ReconnectionException()
        {
        }

        public ReconnectionException(string message) : base(message)
        {
        }
    }
}
