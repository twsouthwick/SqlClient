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

        protected override void GenerateSspiClientContext(ReadOnlyMemory<byte> incomingBlob, IBufferWriter<byte> outgoingBlobWriter)
        {
            _sspiClientContextStatus ??= new SspiClientContextStatus();

            SNIProxy.GenSspiClientContext(_sspiClientContextStatus, incomingBlob, outgoingBlobWriter, new[] { AuthenticationParameters.ServerName });
            SqlClientEventSource.Log.TryTraceEvent("TdsParserStateObjectManaged.GenerateSspiClientContext | Info | Session Id {0}", _physicalStateObj.SessionId);
        }
    }
}
#endif
