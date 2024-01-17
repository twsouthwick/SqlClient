using System;
using System.Buffers;
using System.Diagnostics;

#nullable enable

namespace Microsoft.Data.SqlClient
{
    internal sealed class NativeSSPIContextProvider : SSPIContextProvider
    {
        private static readonly object s_tdsParserLock = new();

        // bool to indicate whether library has been loaded
        private static bool s_fSSPILoaded;

        // variable to hold max SSPI data size, keep for token from server
        private volatile static uint s_maxSSPILength;

        private protected override void Initialize()
        {
            LoadSSPILibrary();
        }

        private void LoadSSPILibrary()
        {
            // Outer check so we don't acquire lock once it's loaded.
            if (!s_fSSPILoaded)
            {
                lock (s_tdsParserLock)
                {
                    // re-check inside lock
                    if (!s_fSSPILoaded)
                    {
                        // use local for ref param to defer setting s_maxSSPILength until we know the call succeeded.
                        uint maxLength = 0;

                        if (0 != SNINativeMethodWrapper.SNISecInitPackage(ref maxLength))
                            SSPIError(SQLMessage.SSPIInitializeError(), TdsEnums.INIT_SSPI_PACKAGE);

                        s_maxSSPILength = maxLength;
                        s_fSSPILoaded = true;
                    }
                }
            }

            if (s_maxSSPILength > int.MaxValue)
            {
                throw SQL.InvalidSSPIPacketSize();   // SqlBu 332503
            }
        }

        internal override IMemoryOwner<byte> GenerateSspiClientContext(ReadOnlyMemory<byte> receivedBuff, byte[][] _sniSpnBuffer)
        {
#if NETFRAMEWORK
            SNIHandle handle = _physicalStateObj.Handle;
#else
            Debug.Assert(_physicalStateObj.SessionHandle.Type == SessionHandle.NativeHandleType);
            SNIHandle handle = _physicalStateObj.SessionHandle.NativeHandle;
#endif
            var length = s_maxSSPILength;
            var buffer = new ArrayPoolOwner((int)length);

            if (0 != SNINativeMethodWrapper.SNISecGenClientContext(handle, receivedBuff.Span, buffer.Memory.Span, ref length, _sniSpnBuffer[0]))
            {
                buffer.Dispose();
                throw new InvalidOperationException(SQLMessage.SSPIGenerateError());
            }

            buffer.Length = (int)length;
            return buffer;
        }

        private sealed class ArrayPoolOwner : IMemoryOwner<byte>
        {
            private readonly byte[] _array;
            private int _length;

            public int Length
            {
                get => _length;
                set
                {
                    if (value < 0 || value > _array.Length)
                    {
                        throw new ArgumentOutOfRangeException(nameof(value));
                    }

                    _length = value;
                }
            }

            public ArrayPoolOwner(int length)
            {
                _array = ArrayPool<byte>.Shared.Rent(length);
                Length = length;
            }

            public Memory<byte> Memory => _array.AsMemory(_length);

            public void Dispose()
            {
                ArrayPool<byte>.Shared.Return(_array);
            }
        }
    }
}

