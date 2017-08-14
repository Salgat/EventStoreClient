using EventStoreClient.Connection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace EventStoreClient.Test
{
    [TestClass]
    public class Connection
    {
        [TestMethod]
        public async Task Connection_Success()
        {
            var credentials = new UserCredentials("admin", "changeit");
            var connectionSettings = new ConnectionSettings(credentials, "127.0.0.1", 1113, "myConnection");
            var connection = new EventStoreConnection(connectionSettings);

            await connection.ConnectAsync().ConfigureAwait(false);

            int count = 0;
            while (true) await Task.Delay(1000).ConfigureAwait(false);
        }
    }
}
