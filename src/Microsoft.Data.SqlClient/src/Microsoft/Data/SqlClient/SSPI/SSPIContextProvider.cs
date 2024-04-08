using System;
using System.Buffers;
using System.Diagnostics;
using Microsoft.Data.Common;

#nullable enable

namespace Microsoft.Data.SqlClient
{
    internal abstract class SSPIContextProvider
    {
        private TdsParser _parser = null!;
        private ServerInfo _serverInfo = null!;
        private protected TdsParserStateObject _physicalStateObj = null!;
        private SqlAuthenticationParameters? _parameters;

        internal void Initialize(ServerInfo serverInfo, TdsParserStateObject physicalStateObj, TdsParser parser, string[] serverNames)
        {
            Debug.Assert(serverNames.Length > 0);

            Initialize(serverInfo, physicalStateObj, parser, serverNames[0]);
        }

        internal void Initialize(ServerInfo serverInfo, TdsParserStateObject physicalStateObj, TdsParser parser, string serverName)
        {
            _parser = parser;
            _physicalStateObj = physicalStateObj;
            _serverInfo = serverInfo;

            var options = parser.Connection.ConnectionOptions;

            _parameters = new SqlAuthenticationParameters.Builder(
                           authenticationMethod: parser.Connection.ConnectionOptions.Authentication,
                           resource: serverName,
                           authority: null,
                           serverName: options.DataSource,
                           databaseName: options.InitialCatalog)
                       .WithConnectionId(parser.Connection.ClientConnectionId)
                       .WithConnectionTimeout(options.ConnectTimeout)
                       .WithUserId(options.UserID)
                       .WithPassword(options.Password);

            Initialize();
        }

        private protected virtual void Initialize()
        {
        }

        /// <summary>
        /// Gets the authentication parameters for the current connection.
        /// </summary>
        protected SqlAuthenticationParameters AuthenticationParameters => _parameters ?? throw new InvalidOperationException("SSPI context provider has not been initialized");

        protected abstract void GenerateSspiClientContext(ReadOnlyMemory<byte> incomingBlob, IBufferWriter<byte> outgoingBlobWriter);

        internal void SSPIData(ReadOnlyMemory<byte> receivedBuff, IBufferWriter<byte> outgoingBlobWriter)
        {
            try
            {
                GenerateSspiClientContext(receivedBuff, outgoingBlobWriter);
            }
            catch (Exception e)
            {
                SSPIError(e.Message + Environment.NewLine + e.StackTrace, TdsEnums.GEN_CLIENT_CONTEXT);
            }
        }

        protected void SSPIError(string error, string procedure)
        {
            Debug.Assert(!ADP.IsEmpty(procedure), "TdsParser.SSPIError called with an empty or null procedure string");
            Debug.Assert(!ADP.IsEmpty(error), "TdsParser.SSPIError called with an empty or null error string");

            _physicalStateObj.AddError(new SqlError(0, (byte)0x00, (byte)TdsEnums.MIN_ERROR_CLASS, _serverInfo.ResolvedServerName, error, procedure, 0));
            _parser.ThrowExceptionAndWarning(_physicalStateObj);
        }
    }
}
