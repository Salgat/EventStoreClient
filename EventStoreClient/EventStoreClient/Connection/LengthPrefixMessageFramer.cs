/*
Event Store License

Copyright (c) 2011-2014, Event Store LLP. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

Neither the name of Event Store LLP nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

https://github.com/EventStore/EventStore
*/

using EventStoreClient.Exceptions;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventStoreClient.Connection
{
    internal class LengthPrefixMessageFramer
    {
        private readonly int _maxPackageSize;
        public const int HeaderLength = sizeof(Int32);

        private byte[] _messageBuffer;
        private int _bufferIndex;
        private Action<ArraySegment<byte>> _receivedHandler;

        private int _headerBytes;
        private int _packageLength;

        /// <summary>
        /// Initializes a new instance of the <see cref="LengthPrefixMessageFramer"/> class.
        /// </summary>
        public LengthPrefixMessageFramer(Action<ArraySegment<byte>> handler = null, int maxPackageSize = 64 * 1024 * 1024)
        {
            _maxPackageSize = maxPackageSize;
            _receivedHandler = handler;
        }

        public void Reset()
        {
            _messageBuffer = null;
            _headerBytes = 0;
            _packageLength = 0;
            _bufferIndex = 0;
        }
        
        public void UnFrameData(ArraySegment<byte> data)
        {
            Parse(data);
        }

        /// <summary>
        /// Parses a stream chunking based on length-prefixed framing. Calls are re-entrant and hold state internally.
        /// </summary>
        /// <param name="bytes">A byte array of data to append</param>
        private void Parse(ArraySegment<byte> bytes)
        {
            byte[] data = bytes.Array;
            for (int i = bytes.Offset; i < bytes.Offset + bytes.Count; i++)
            {
                if (_headerBytes < HeaderLength)
                {
                    _packageLength |= (data[i] << (_headerBytes * 8)); // little-endian order
                    ++_headerBytes;
                    if (_headerBytes != HeaderLength) continue;
                    if (_packageLength <= 0 || _packageLength > _maxPackageSize)
                        throw new PackageFramingException(string.Format("Package size is out of bounds: {0} (max: {1}). This is likely an exceptionally large message (reading too many things) or there is a problem with the framing if working on a new client.", _packageLength, _maxPackageSize));

                    _messageBuffer = new byte[_packageLength];
                }
                else
                {
                    int copyCnt = Math.Min(bytes.Count + bytes.Offset - i, _packageLength - _bufferIndex);
                    Buffer.BlockCopy(bytes.Array, i, _messageBuffer, _bufferIndex, copyCnt);
                    _bufferIndex += copyCnt;
                    i += copyCnt - 1;

                    if (_bufferIndex == _packageLength)
                    {
                        _receivedHandler?.Invoke(new ArraySegment<byte>(_messageBuffer, 0, _bufferIndex));
                        _messageBuffer = null;
                        _headerBytes = 0;
                        _packageLength = 0;
                        _bufferIndex = 0;
                    }
                }
            }
        }

        public static ArraySegment<byte> FrameData(ArraySegment<byte> data)
        {
            var length = data.Count;
            var array = new byte[length+4];
            array[0] = (byte)length;
            array[1] = (byte)(length >> 8);
            array[2] = (byte)(length >> 16);
            array[3] = (byte)(length >> 24);
            Buffer.BlockCopy(data.Array, 0, array, 4, length);
            return new ArraySegment<byte>(array, 0, length + 4);
        }
    }
}
