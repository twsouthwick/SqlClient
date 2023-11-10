using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace NetCoreNtlmTest
{
    internal class DbTest
    {
        public DbTest() { }

        public void test()
        {
            //Initial Catalog=test-db;
            var connString = "Server=tcp:localhost,1433;Encrypt=False;TrustServerCertificate=True;Connection Timeout=30;Integrated Security=true;User ID=DOMAIN\\ntlmuser;Password=mypassword;";

            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();

                String sql = "SELECT 'hello world'";

                using (SqlCommand command = new SqlCommand(sql, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Console.WriteLine("Got data: "+ reader.GetString(0));
                        }
                    }
                }
            }
        }
    }
}
