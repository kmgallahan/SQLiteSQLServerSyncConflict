using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Microsoft.Data.Sqlite;
using Microsoft.Data.SqlClient;

namespace SQLiteSyncClient
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // 1. Check location of .exe and define SQLite DB path
            var exePath = AppContext.BaseDirectory;
            var dbName = "client.db";
            var dbPath = Path.Combine(exePath, dbName);
            var clientConnectionString = $"Data Source={dbPath}";
            var ServerConnectionString = (new SqlConnectionStringBuilder()
            {
                DataSource = "localhost",
                InitialCatalog = "DotmimConflict",
                UserID = "sa",
                Password = "",
                TrustServerCertificate = true,
            })
            .ConnectionString;

            Console.WriteLine($"Client Database Path: {dbPath}");
            Console.WriteLine($"Server Connection String: {ServerConnectionString}");

            // Ensure database file exists
            if (!File.Exists(dbPath))
            {
                Console.WriteLine("Creating new SQLite database...");
                // Just touching the file or opening a connection will create it
                using (var connection = new SqliteConnection(clientConnectionString))
                {
                    connection.Open();
                }
            }

            if(await EnsureServerTableExistsAsync() is false)
            {
                return;
            }

            // 2 & 3. Define Providers
            var clientProvider = new SqliteSyncProvider(clientConnectionString);
            var serverProvider = new SqlSyncChangeTrackingProvider(ServerConnectionString);

            // Setup Sync Agent
            var setup = new SyncSetup("Customer");
            var agent = new SyncAgent(clientProvider, serverProvider)
            {
                Options =
                {
                    UseVerboseErrors = true,
                },
            };

            // Run initial sync
            await agent.SynchronizeAsync(setup);

            // 4. Check for Customer entity and create if missing
            await EnsureCustomerExistsAsync(clientConnectionString);

            var syncLock = new System.Threading.SemaphoreSlim(1, 1);
            async Task RunSyncAsync()
            {
                try
                {
                    await syncLock.WaitAsync();
                    var result = await agent.SynchronizeAsync(setup);
                    Console.WriteLine($"[{DateTime.Now}] Sync Result: {result}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[{DateTime.Now}] Sync Error: {ex.Message}");
                }
                finally
                {
                    syncLock.Release();
                }
            }

            // Start the synchronization loop in a background task
            var syncTask = Task.Run(async () =>
            {
                while (true)
                {
                    // 5. Run SynchronizeAsync every 10 seconds
                    await RunSyncAsync();
                    await Task.Delay(TimeSpan.FromSeconds(10));
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
                await RunSyncAsync();
            }

            return;

            async Task<bool> EnsureServerTableExistsAsync()
            {
                try
                {
                    var masterBuilder = new SqlConnectionStringBuilder(ServerConnectionString)
                    {
                        InitialCatalog = "master"
                    };

                    using (var masterConnection = new SqlConnection(masterBuilder.ConnectionString))
                    {
                        await masterConnection.OpenAsync();

                        var cmd = masterConnection.CreateCommand();
                        cmd.CommandText = "SELECT db_id('DotmimConflict')";
                        var dbId = await cmd.ExecuteScalarAsync();

                        if (dbId == DBNull.Value)
                        {
                            Console.WriteLine("Database 'DotmimConflict' does not exist. Creating...");
                            cmd.CommandText = "CREATE DATABASE DotmimConflict";
                            await cmd.ExecuteNonQueryAsync();
                            Console.WriteLine("Database 'DotmimConflict' created.");
                        }

                        cmd.CommandText = "SELECT count(*) FROM sys.change_tracking_databases WHERE database_id = DB_ID('DotmimConflict')";
                        if ((int)await cmd.ExecuteScalarAsync() == 0)
                        {
                            Console.WriteLine("Enabling Change Tracking on 'DotmimConflict'...");
                            cmd.CommandText = @"ALTER DATABASE DotmimConflict
                                                SET CHANGE_TRACKING = ON
                                                (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)";
                            await cmd.ExecuteNonQueryAsync();
                            Console.WriteLine("Change Tracking enabled.");
                        }
                    }

                    using (var connection = new SqlConnection(ServerConnectionString))
                    {
                        await connection.OpenAsync();

                        var checkCmd = connection.CreateCommand();
                        checkCmd.CommandText = "SELECT count(*) FROM sys.tables WHERE name = 'Customer' AND type = 'U'";
                        if ((int)await checkCmd.ExecuteScalarAsync() == 0)
                        {
                            checkCmd.CommandText = @"
                        CREATE TABLE Customer (
                            Id INT PRIMARY KEY IDENTITY(1,1),
                            FirstName NVARCHAR(50),
                            LastName NVARCHAR(50)
                        )";
                            await checkCmd.ExecuteNonQueryAsync();
                            Console.WriteLine("'Customer' table created on server.");
                        }

                        return true;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unable to connect to SQL Server. Error: {ex.Message}");
                    return false;
                }
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