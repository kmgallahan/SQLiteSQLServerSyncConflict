using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Microsoft.Data.Sqlite;
using System;
using System.Data;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SQLiteSyncClient
{
    class Program
    {

        static async Task Main(string[] args)
        {
            // 1. Check location of .exe and define SQLite DB path
            var exePath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            var dbName = "client.db";
            var dbPath = Path.Combine(exePath, dbName);
            var clientConnectionString = $"Data Source={dbPath}";

            var ServerConnectionString = (new SqlConnectionStringBuilder()
                {
                    DataSource = "192.168.0.1,1433",
                    InitialCatalog = "ConflictProgram",
                    AttachDBFilename = "C:\\Dev\\Sandbox\\SQLDB\\DotmimConflict.mdf",
                    UserID = "sa",
                    Password = "",
                    TrustServerCertificate = true,
                })
                .ConnectionString;

            Console.WriteLine($"Client Database Path: {dbPath}");

            // Ensure database file exists
            if (!File.Exists(dbPath))
            {
                Console.WriteLine("Creating new SQLite database...");
                // Just touching the file or opening a connection will create it
                using (var connection = new SqliteConnection(clientConnectionString))
                {
                    connection.Open();
                    // Create the Customer table explicitly if we are starting fresh
                    var cmd = connection.CreateCommand();
                    cmd.CommandText = "CREATE TABLE IF NOT EXISTS Customer (Id INTEGER PRIMARY KEY AUTOINCREMENT, FirstName TEXT, LastName TEXT);";
                    cmd.ExecuteNonQuery();
                }
            }

            // 2 & 3. Define Providers
            var clientProvider = new SqliteSyncProvider(clientConnectionString);
            var serverProvider = new SqlSyncChangeTrackingProvider(ServerConnectionString);

            // Setup Sync Agent
            var tables = new string[] { "Customer" };
            var agent = new SyncAgent(clientProvider, serverProvider)
            {
                Options =
                {
                    UseVerboseErrors = true,
                },
            };

            // Run initial sync
            await agent.SynchronizeAsync();

            // 4. Check for Customer entity and create if missing
            await EnsureCustomerExistsAsync(clientConnectionString);

            // Start the synchronization loop in a background task
            var syncTask = Task.Run(async () =>
            {
                while (true)
                {
                    try
                    {
                        // 5. Run SynchronizeAsync every 20 seconds
                        var result = await agent.SynchronizeAsync();
                        Console.WriteLine($"[{DateTime.Now}] Sync Result: {result}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[{DateTime.Now}] Sync Error: {ex.Message}");
                    }

                    await Task.Delay(TimeSpan.FromSeconds(20));
                }
            });

            // 6. User interaction loop
            Console.WriteLine("\n*** Press any key to modify the Customer's Last Name. Press 'Esc' to exit. ***\n");

            while (true)
            {
                var key = Console.ReadKey(true);
                if (key.Key == ConsoleKey.Escape)
                {
                    break;
                }

                await ModifyCustomerLastNameAsync(clientConnectionString);
            }
        }

        private static async Task EnsureCustomerExistsAsync(string connectionString)
        {
            using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();

                var cmd = connection.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM Customer";
                var count = (long)await cmd.ExecuteScalarAsync();

                if (count == 0)
                {
                    Console.WriteLine("No customer found. Creating default customer...");
                    var insertCmd = connection.CreateCommand();
                    insertCmd.CommandText = "INSERT INTO Customer (FirstName, LastName) VALUES (@fn, @ln)";
                    insertCmd.Parameters.AddWithValue("@fn", "John");
                    insertCmd.Parameters.AddWithValue("@ln", "Doe");
                    await insertCmd.ExecuteNonQueryAsync();
                }
            }
        }

        private static async Task ModifyCustomerLastNameAsync(string connectionString)
        {
            var random = new Random();
            var newLastName = $"Doe_{random.Next(1000, 9999)}";

            using (var connection = new SqliteConnection(connectionString))
            {
                await connection.OpenAsync();
                var cmd = connection.CreateCommand();
                // Updates the first customer found
                cmd.CommandText = "UPDATE Customer SET LastName = @ln WHERE Id = (SELECT Id FROM Customer LIMIT 1)";
                cmd.Parameters.AddWithValue("@ln", newLastName);

                var rows = await cmd.ExecuteNonQueryAsync();
                if (rows > 0)
                {
                    Console.WriteLine($"\n-> Updated Customer LastName to '{newLastName}' locally.");
                }
            }
        }
    }
}