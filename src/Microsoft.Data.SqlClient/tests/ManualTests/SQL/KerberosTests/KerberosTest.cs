// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using Xunit;

namespace Microsoft.Data.SqlClient.ManualTesting.Tests
{
    public class KerberosTests
    {
        [PlatformSpecific(TestPlatforms.AnyUnix)]
        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsKerberosTest))]
        [ClassData(typeof(ConnectionStringsProvider))]
        public void IsKerBerosSetupTestAsync(string connectionStr)
        {
            KerberosTicketManagemnt.Init(DataTestUtility.KerberosDomainUser, DataTestUtility.KerberosDomainPassword);
            using SqlConnection conn = new(connectionStr);

            conn.Open();
            using SqlCommand command = new("SELECT auth_scheme from sys.dm_exec_connections where session_id = @@spid", conn);
            using SqlDataReader reader = command.ExecuteReader();
            Assert.True(reader.Read(), "Expected to receive one row data");
            Assert.Equal("KERBEROS", reader.GetString(0));
        }

        [PlatformSpecific(TestPlatforms.AnyUnix)]
        [ConditionalTheory(typeof(DataTestUtility), nameof(DataTestUtility.IsKerberosTest))]
        [ClassData(typeof(ConnectionStringsProvider))]
        public void CustomSSPIContextProviderIsUsed(string connectionStr)
        {
            KerberosTicketManagemnt.Init(DataTestUtility.KerberosDomainUser, DataTestUtility.KerberosDomainPassword);
            using SqlConnection conn = new(connectionStr)
            {
                SSPIContextProviderFactory = () => new ThrowingSspiContextProvider()
            };

            Assert.Throws<SSPIException>(() =>
            {
                // TODO: is open enought?
                conn.Open();
                using SqlCommand command = new("SELECT auth_scheme from sys.dm_exec_connections where session_id = @@spid", conn);
                using SqlDataReader reader = command.ExecuteReader();
            });
        }

        private sealed class ThrowingSspiContextProvider : SSPIContextProvider
        {
            protected override void GenerateSspiClientContext(ReadOnlyMemory<byte> incomingBlob, IBufferWriter<byte> outgoingBlobWriter)
            {
                throw new SSPIException();
            }
        }

        private sealed class SSPIException : Exception
        {
        }
    }

    public class ConnectionStringsProvider : IEnumerable<object[]>
    {
        public IEnumerator<object[]> GetEnumerator()
        {
            foreach (var cnnString in DataTestUtility.ConnectionStrings)
            {
                yield return new object[] { cnnString };
            }
        }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
