using EventStoreClient.Exceptions;
using System;
using System.Threading.Tasks;

namespace EventStoreClient.Connection
{
    internal partial class ConnectionManager
    {
        private async Task<T> AttemptOperation<T>(Func<Task<T>> operation, Func<Exception, Task> onFailure = null)
        {
            var attempts = 0;
            while (_connected)
            {
                try
                {
                    return await operation().ConfigureAwait(false);
                }
                catch (ReconnectionException ex)
                {
                    if (onFailure != null) await onFailure(ex).ConfigureAwait(false);
                    if (_settings.MaxOperationRetries == -1 || attempts++ < _settings.MaxOperationRetries) throw;
                }
            }
            throw new DisconnectedException();
        }

        private async Task AttemptOperation(Func<Task> operation, Func<Exception, Task> onFailure = null)
        {
            var attempts = 0;
            while (_connected)
            {
                try
                {
                    await operation().ConfigureAwait(false);
                    return;
                }
                catch (ReconnectionException ex)
                {
                    if (onFailure != null) await onFailure(ex).ConfigureAwait(false);
                    if (_settings.MaxOperationRetries == -1 || attempts++ < _settings.MaxOperationRetries) throw;
                }
            }
            throw new DisconnectedException();
        }
    }
}
