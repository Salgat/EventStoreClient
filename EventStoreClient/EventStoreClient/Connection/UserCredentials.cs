using System;
using System.Collections.Generic;
using System.Text;

namespace EventStoreClient.Connection
{
    public sealed class UserCredentials
    {
        public readonly string Username;
        public readonly string Password;

        public UserCredentials(string username, string password)
        {
            Username = username;
            Password = password;
        }
    }
}
