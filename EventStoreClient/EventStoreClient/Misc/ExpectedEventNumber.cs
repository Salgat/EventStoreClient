using System;
using System.Collections.Generic;
using System.Text;

namespace EventStoreClient.Misc
{
    public static class ExpectedEventNumber
    {
        /// <summary>
        /// This write should not conflict with anything and should always succeed.
        /// </summary>
        public const int Any = -2;
        /// <summary>
        /// The stream being written to should not yet exist. If it does exist treat that as a concurrency problem.
        /// </summary>
        public const int NoStream = -1;
        /// <summary>
        /// The stream should exist and should be empty. If it does not exist or is not empty treat that as a concurrency problem.
        /// </summary>
        public const int EmptyStream = -1;

        /// <summary>
        /// The stream should exist. If it or a metadata stream does not exist treat that as a concurrency problem.
        /// </summary>
        public const int StreamExists = -4;
    }
}
