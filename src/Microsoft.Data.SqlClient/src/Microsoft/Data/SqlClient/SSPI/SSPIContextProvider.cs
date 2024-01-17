using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Data.Common;

#nullable enable

namespace Microsoft.Data.SqlClient
{
    /// <summary>
    /// A provider to generate SSPI contexts
    /// </summary>
    public abstract class SSPIContextProvider
    {
        private TdsParser _parser = null!;
        private ServerInfo _serverInfo = null!;
        private protected TdsParserStateObject _physicalStateObj = null!;
        private string[] _serverNames = Array.Empty<string>();

        internal void Initialize(ServerInfo serverInfo, TdsParserStateObject physicalStateObj, TdsParser parser, string[] serverNames)
        {
            _parser = parser;
            _physicalStateObj = physicalStateObj;
            _serverInfo = serverInfo;
            _serverNames = serverNames;

            Debug.Assert(_serverNames.Length > 0);

            Initialize();
        }

        private protected virtual void Initialize()
        {
        }

        /// <summary>
        /// Gets a readonly list of server names we are connecting to.
        /// </summary>
        public IReadOnlyList<string> ServerNames => _serverNames;

        /// <summary>
        /// Method to generate SSPI client context blobs.
        /// </summary>
        /// <param name="input">Received buffer, if any.</param>
        /// <returns>A memory owned type with the response of the client.</returns>
        protected abstract IMemoryOwner<byte> GenerateSspiClientContext(ReadOnlyMemory<byte> input);

        internal IMemoryOwner<byte> SSPIData(ReadOnlyMemory<byte> receivedBuff)
        {
            try
            {
                return GenerateSspiClientContext(receivedBuff);
            }
            catch (Exception e)
            {
                SSPIError(e.Message + Environment.NewLine + e.StackTrace, TdsEnums.GEN_CLIENT_CONTEXT);
                throw; // SSPIError should throw, but just in case
            }
        }

        private protected void SSPIError(string error, string procedure)
        {
            Debug.Assert(!ADP.IsEmpty(procedure), "TdsParser.SSPIError called with an empty or null procedure string");
            Debug.Assert(!ADP.IsEmpty(error), "TdsParser.SSPIError called with an empty or null error string");

            _physicalStateObj.AddError(new SqlError(0, (byte)0x00, (byte)TdsEnums.MIN_ERROR_CLASS, _serverInfo.ResolvedServerName, error, procedure, 0));
            _parser.ThrowExceptionAndWarning(_physicalStateObj);
        }
    }
}
