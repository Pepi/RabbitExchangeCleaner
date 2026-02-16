using RabbitExchangeCleaner.Utilities;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.CommandLine;
using System.CommandLine.Help;
using System.CommandLine.Invocation;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;


namespace RabbitExchangeCleaner
{
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            Console.WriteLine("Hello, World!\n\r");

            CultureInfo.CurrentUICulture = new CultureInfo("it-IT");


            #region Parmeters definition

            var hostOption = new Option<string>("--host", "-h")
            {
                Description = "Indirizzo del server RabbitMQ",
                DefaultValueFactory = parseResult => "localhost"
            };
            var portOption = new Option<int>("--port", "-p")
            {
                Description = "Porta del server RabbitMQ",
                DefaultValueFactory = parseResult => 15672
            };

            var userOption = new Option<string>("--user", "-u")
            {
                Description = "Username RabbitMQ",
                DefaultValueFactory = parseResult => "guest"
            };

            var passOption = new Option<string>("--password", "-w")
            {
                Description = "Password RabbitMQ",
                DefaultValueFactory = parseResult => "guest"
            };

            var vHostOption = new Option<string>("--vhost", "-vh")
            {
                Description = "Virtual Host [default: tutti] ",
                DefaultValueFactory = parseResult => string.Empty
            };

            var prefixesOption = new Option<string[]>("--names", "-n")
            {
                Description = "Lista dei prefissi degli exchange da cancellare",
                Required = true,
                AllowMultipleArgumentsPerToken = true
            };

            var confirmOption = new Option<bool>("--confirm", "-c")
            {
                Description = "Conferma ogni cancellazione",
                DefaultValueFactory = parseResult => false
            };
            var previewOption = new Option<bool>("--preview")
            {
                Description = "Elenca gli exchange che verranno cancellati",
                DefaultValueFactory = parseResult => false
            };

            #endregion

            var rootCommand =
                new RootCommand("Utility per cancellare Exchange RabbitMQ basati su prefissi.")
                {
                    hostOption,
                    portOption,
                    userOption,
                    passOption,
                    vHostOption,
                    prefixesOption,
                    confirmOption,
                    previewOption
                };

            var option = rootCommand.Options.FirstOrDefault(o => o is HelpOption);
            if (option != null)
                option.Description = "Mostra informazioni di aiuto e utilizzo";

            option = rootCommand.Options.FirstOrDefault(o => o is VersionOption);
            if (option != null)
                option.Description = "Mostra informazioni sulla versione";


            rootCommand.SetAction(async (result, token) =>
            {
                var host = result.GetValue(hostOption);
                var port = result.GetValue<int>(portOption);
                var user = result.GetValue(userOption);
                var pass = result.GetValue(passOption);
                var prefixes = result.GetValue(prefixesOption)!;
                var vhost = result.GetValue(vHostOption)!;
                var preview = result.GetValue<bool>(previewOption);
                var confirm = result.GetValue<bool>(confirmOption);

                await CleanExchangesAsync(host, port, user, pass, vhost, prefixes, preview, confirm);
            });

            var parseResult = rootCommand.Parse(args);

            if (!parseResult.Tokens.Any())
            {
                // richiesta manuale dei parametri
                ConsoleExt.WriteLine(ConsoleColor.Yellow, "Nessun parametro rilevato. Avvio modalità interattiva...\n\r");

                var tmpArg = new List<string>();

                Console.Write("Host RabbitMQ [localhost]: ");
                var host = Console.ReadLine();
                if (string.IsNullOrEmpty(host))
                    host = "localhost";

                tmpArg.Add("--host");
                tmpArg.Add(host);


                Console.Write("Porta RabbitMQ [15672]: ");
                var portInput = Console.ReadLine();
                var port = 15672;
                if (!string.IsNullOrEmpty(portInput) && int.TryParse(portInput, out var parsedPort))
                    port = parsedPort;

                tmpArg.Add("--port");
                tmpArg.Add(port.ToString());


                Console.Write("Username [guest]: ");
                var user = Console.ReadLine();
                if (string.IsNullOrEmpty(user))
                    user = "guest";

                tmpArg.Add("--user");
                tmpArg.Add(user);

                Console.Write("Password [guest]: ");
                var pass = Console.ReadLine();
                if (string.IsNullOrEmpty(pass))
                    pass = "guest";

                tmpArg.Add("--password");
                tmpArg.Add(pass);


                Console.Write("Virtual Host (lascia vuoto per tutti): ");
                var vhost = Console.ReadLine();

                if (!string.IsNullOrEmpty(vhost))
                {
                    tmpArg.Add("--vhost");
                    tmpArg.Add(vhost);
                }


                Console.Write("Prefissi exchange da cancellare (separati da spazio o virgola): ");
                var prefixesInput = Console.ReadLine();
                var prefixes = Array.Empty<string>();
                if (!string.IsNullOrEmpty(prefixesInput))
                {
                    prefixes = prefixesInput
                        .Split([' ', ','], StringSplitOptions.RemoveEmptyEntries)
                        .Select(p => p.Trim())
                        .ToArray();
                }
                if (prefixes.Length == 0)
                {
                    ConsoleExt.WriteLine(ConsoleColor.Red, "Nessun prefisso specificato. Operazione annullata.");
                    return 2;
                }

                tmpArg.Add("--names");
                tmpArg.AddRange(prefixes);

                var how = GetYesNoInput("Elencare gli Exchange senza cancellarli?:");
                tmpArg.Add("--preview");
                tmpArg.Add(how.ToString());

                if (!how)
                {
                    how = GetYesNoInput("Chiedere conferma prima di ogni cancellazione?:");

                    tmpArg.Add("--confirm");
                    tmpArg.Add(how.ToString());
                }

                parseResult = rootCommand.Parse(tmpArg.ToArray());
            }

            if (parseResult.Errors.Count <= 0)
                return await parseResult.InvokeAsync();

            // Gestione errori di parsing
            foreach (var error in parseResult.Errors)
            {
                ConsoleExt.WriteLine(ConsoleColor.Red, error.Message);
            }
            return 1;

        }

        private static async Task CleanExchangesAsync(string? host, int port, string? username, string? password,
            string? vHost,
            string[]? prefixes, bool preview, bool confirm)
        {

            var vHostSpecified = !string.IsNullOrEmpty(vHost);

            var labelAzione = preview ? "Elenco exchange" : "Avvio pulizia";
            var labelDisplay = preview ? "elencare" : "cancellare";
            var labelDelete = preview ? "[SARA' ELIMINATO]" : "[ELIMINATO]";

            if (preview)
                confirm = false;

            ConsoleExt.WriteLine(ConsoleColor.Cyan, $"{labelAzione} su {host}...\n\r" +
                                                    (vHostSpecified ? $"VHost: {vHost}\n\r" : "") +
                                                    $"Prefissi target: {string.Join(", ", prefixes!)}");



            // 1. Setup HttpClient
            using var httpClient = new HttpClient();
            // Impostiamo l'indirizzo base per comodità
            httpClient.BaseAddress = new Uri($"http://{host}:{port}/api/");

            // Configurazione Basic Auth
            var authString = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{username}:{password}"));
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", authString);

            try
            {
                // Esecuzione richiesta GET
                var response = await httpClient.GetAsync("exchanges");
                //var response = await httpClient.GetAsync("queues");   // il browsing funziona lo stesso per le code senza cambiare oggetto di mapping

                if (!response.IsSuccessStatusCode)
                {
                    ConsoleExt.WriteLine(ConsoleColor.Red, $"\n\rErrore API Management: {response.StatusCode} - {response.ReasonPhrase}");
                    return;
                }

                // 2. Parsing JSON con System.Text.Json
                var contentStream = await response.Content.ReadAsStreamAsync();

                // Usiamo JsonNode per un parsing dinamico simile a JArray di Newtonsoft
                var rootNode = await JsonNode.ParseAsync(contentStream);

                if (rootNode is not JsonArray exchangesArray)
                {
                    ConsoleExt.WriteLine(ConsoleColor.Red, "\n\rFormato risposta imprevisto (non è un array).");

                    return;
                }

                // Mapping JSON -> Oggetto InfoToken
                var allExchanges = exchangesArray.Select(node => new InfoToken
                {
                    // System.Text.Json è case-sensitive di default, ma l'API RabbitMQ restituisce minuscolo
                    Name = node?["name"]?.ToString(),
                    VHost = node?["vhost"]?.ToString()
                });

                // 3. Filtro in memoria

                var wildcardRegexes = prefixes!
                    .Select(p =>
                    {
                        p = p.Trim() + "*"; // Aggiungiamo jolly finale se non presente 
                        // Escape dei caratteri speciali, poi conversione dei jolly
                        var pattern = "^" + Regex.Escape(p)
                            .Replace("\\*", ".*")
                            .Replace("\\?", ".") + "$";


                        // RegexOptions.Compiled aumenta le performance nel loop successivo
                        return new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);
                    })
                    .ToList();

                var temporary = allExchanges
                    .Where(t => !string.IsNullOrEmpty(t.Name) &&
                                wildcardRegexes.Any(r => r.IsMatch(t.Name)));

                if (vHostSpecified)
                    temporary = temporary.Where(t => t.VHost == vHost);

                var exchangesToDelete = temporary
                    .Where(t => !string.IsNullOrEmpty(t.Name) &&
                                wildcardRegexes.Any(r => r.IsMatch(t.Name)))
                    .ToList();


                if (exchangesToDelete.Count == 0)
                {
                    ConsoleExt.WriteLine(ConsoleColor.Red, "\n\rNessun exchange trovato con i prefissi specificati.");
                    return;
                }

                Console.WriteLine($"Trovati {exchangesToDelete.Count} exchange da {labelDisplay}.");

                // 4. Cancellazione effettiva (RabbitMQ.Client)
                var groupedByVHost = exchangesToDelete.GroupBy(i => i.VHost);

                foreach (var group in groupedByVHost)
                {
                    var currentVHost = group.Key!;

                    try
                    {
                        ConsoleExt.WriteLine(ConsoleColor.Blue, $"Connessione al VHost: '{currentVHost}'...");

                        var factory = new ConnectionFactory
                        {
                            HostName = host!,
                            UserName = username!,
                            Password = password!,
                            VirtualHost = currentVHost
                        };

                        await using var connection = await factory.CreateConnectionAsync();
                        await using var channel = await connection.CreateChannelAsync();

                        foreach (var exchangeToken in group)
                        {
                            try
                            {
                                if (confirm)
                                {
                                    var userConfirmed = GetYesNoInput($"Confermi la cancellazione dell'exchange '{exchangeToken.Name}' sul VHost '{currentVHost}'?");
                                    if (!userConfirmed)
                                    {
                                        ConsoleExt.WriteLine(ConsoleColor.Yellow, $"[SKIPPED] {exchangeToken}");
                                        continue;
                                    }
                                }

                                if (!preview)
                                {
                                    await channel.ExchangeDeleteAsync(exchangeToken.Name!);
                                }


                                ConsoleExt.WriteLine(ConsoleColor.Green, $"{labelDelete} {exchangeToken}");

                            }
                            catch (Exception ex)
                            {
                                ConsoleExt.WriteLine(ConsoleColor.Yellow, $"[ERRORE DELETE] {exchangeToken.Name}: {ex.Message}");

                            }
                        }
                    }
                    catch (BrokerUnreachableException ex)
                    {
                        ConsoleExt.WriteLine(ConsoleColor.Red, $"Broker irraggiungibile per VHost {currentVHost}: {ex.Message}");
                    }
                }
            }
            catch (HttpRequestException httpEx)
            {
                ConsoleExt.WriteLine(ConsoleColor.Red, $"\n\rErrore HTTP: {httpEx.Message}");
            }
            catch (Exception ex)
            {
                ConsoleExt.WriteLine(ConsoleColor.Red, $"\n\rErrore generale: {ex.Message}");
            }
        }

        /// <summary>
        /// Richiede all'utente una risposta 'S' (Sì) o 'N' (No) e restituisce il booleano corrispondente.
        /// Ripete la richiesta finché l'input non è valido.
        /// </summary>
        /// <param name="promptMessage">Il messaggio da visualizzare all'utente per la richiesta.</param>
        /// <returns>True se l'utente risponde 'S', False se l'utente risponde 'N'.</returns>
        public static bool GetYesNoInput(string promptMessage)
        {
            bool? result = null;

            do
            {
                // Visualizza il messaggio di richiesta e l'indicazione (S/N)
                Console.Write($"{promptMessage} (S/N): ");

                // Legge la riga di input e la converte in maiuscolo per un confronto non sensibile alle maiuscole
                var input = Console.ReadLine()?.ToUpperInvariant();

                if (input == "S")
                {
                    result = true;
                }
                else if (input == "N")
                {
                    result = false;
                }
                else
                {
                    // Opzionale: notifica all'utente che l'input non è valido
                    Console.WriteLine("Input non valido. Si prega di rispondere 'S' per Sì o 'N' per No.");
                }

            } while (!result.HasValue); // Continua finché 'result' non ha un valore (cioè, finché l'input è valido)

            return result.Value;
        }
    }
}
