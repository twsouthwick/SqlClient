// See https://aka.ms/new-console-template for more information
using System;
using System.Threading.Tasks;
using Kerberos.NET.Client;
using Kerberos.NET.Credentials;
using Microsoft.Data.SqlClient;

const string userId = "testUser";
const string password = "IEgXw0iQZiM6DmGy";
const string server = "CPC-tasou-I0BEC";

var strBuilder = new SqlConnectionStringBuilder
{
    UserID = userId,
    Password = password,
    ServerSPN = server,
    IntegratedSecurity = true,
    InitialCatalog = "test",
    TrustServerCertificate = true
};

var str = strBuilder.ToString();

using var connection = new SqlConnection(str);

connection.NegotiateCallback += (args, token) =>
{
    if (args.LastReceived.IsEmpty)
    {
        var type1Message = new SharpCifs.Ntlmssp.Type1Message(); //[review] FLAGS
        type1Message.SetSuppliedWorkstation(args.AuthenticationParameters.ServerName);
        return Task.FromResult<ReadOnlyMemory<byte>>(type1Message.ToByteArray());
    }
    else
    {
        var type2Message = new SharpCifs.Ntlmssp.Type2Message(args.LastReceived.ToArray());
        var userId = args.AuthenticationParameters.UserId;
        var (user, domain) = ParseUser(userId, args.AuthenticationParameters.ServerName);
        var type3Message = new SharpCifs.Ntlmssp.Type3Message(type2Message, args.AuthenticationParameters.Password, domain, user, args.AuthenticationParameters.ServerName, type2Message.GetFlags());

        return Task.FromResult<ReadOnlyMemory<byte>>(type3Message.ToByteArray());
    }

    static (string user, string domain) ParseUser(string input, string defaultDomain)
    {
        var split = input.Split('\\');

        if (split.Length == 2)
        {
            return (split[1], split[0]);
        }
        else
        {
            return (input, defaultDomain);
        }
    }
    //sendLength = (uint)sendBuff.Length;

    //try
    //{
    //    var client = new KerberosClient(new Kerberos.NET.Configuration.Krb5Config { });
    //    await client.Authenticate(new KerberosPasswordCredential(args.UserId, args.Password, args.ServerName));
    //    var context = await client.GetServiceTicket(args.Resource);

    //    return context.EncodeGssApi();
    //}
    //catch (Exception e)
    //{
    //    throw;
    //}
};

connection.Open();
