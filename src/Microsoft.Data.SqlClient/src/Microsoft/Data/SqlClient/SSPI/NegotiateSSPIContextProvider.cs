#if NET7_0_OR_GREATER

using System;
using System.Net.Security;
using System.Buffers;

#nullable enable

namespace Microsoft.Data.SqlClient
{
    internal sealed class NegotiateSSPIContextProvider : SSPIContextProvider
    {
        private NegotiateAuthentication? _negotiateAuth = null;

        internal override IMemoryOwner<byte> GenerateSspiClientContext(ReadOnlyMemory<byte> received)
        {
            _negotiateAuth ??= new(new NegotiateAuthenticationClientOptions { Package = "Negotiate", TargetName = ServerNames[0] });
            var sendBuff = _negotiateAuth.GetOutgoingBlob(received.Span, out NegotiateAuthenticationStatusCode statusCode)!;
            SqlClientEventSource.Log.TryTraceEvent("TdsParserStateObjectManaged.GenerateSspiClientContext | Info | Session Id {0}, StatusCode={1}", _physicalStateObj.SessionId, statusCode);
            if (statusCode is not NegotiateAuthenticationStatusCode.Completed and not NegotiateAuthenticationStatusCode.ContinueNeeded)
            {
                throw new InvalidOperationException(SQLMessage.SSPIGenerateError() + Environment.NewLine + statusCode);
            }
            return sendBuff.AsMemoryOwner();
        }
    }
}
#endif
