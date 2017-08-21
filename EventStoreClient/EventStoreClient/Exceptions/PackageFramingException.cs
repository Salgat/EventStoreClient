using System;
using System.Collections.Generic;
using System.Text;

namespace EventStoreClient.Exceptions
{
    internal class PackageFramingException : Exception
    {
        public PackageFramingException()
        {
        }

        public PackageFramingException(string message)
                : base(message)
        {
        }

        public PackageFramingException(string message, Exception innerException)
                : base(message, innerException)
        {
        }
    }
}
