#if !NETFRAMEWORK && !NET7_0_OR_GREATER

using System;
using System.Buffers;
using Microsoft.Data.SqlClient.SNI;

#nullable enable

namespace Microsoft.Data.SqlClient
{
    internal sealed class ManagedSSPIContextProvider : SSPIContextProvider
    {
        private SspiClientContextStatus? _sspiClientContextStatus;

        internal override IMemoryOwner<byte> GenerateSspiClientContext(ReadOnlyMemory<byte> received, byte[][] _sniSpnBuffer)
        {
            _sspiClientContextStatus ??= new SspiClientContextStatus();

            try
            {
                return SNIProxy.GenSspiClientContext(_sspiClientContextStatus, received, _sniSpnBuffer);
            }
            finally
            {
                SqlClientEventSource.Log.TryTraceEvent("TdsParserStateObjectManaged.GenerateSspiClientContext | Info | Session Id {0}", _physicalStateObj.SessionId);
            }
        }
    }
}
#endif
