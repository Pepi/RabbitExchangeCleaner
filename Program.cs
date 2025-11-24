using RabbitExchangeCleaner.Utilities;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.CommandLine;
using System.Globalization;
using System.Net.Http.Headers;
using System.Runtime.InteropServices.Marshalling;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;


namespace RabbitExchangeCleaner
{
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            Console.WriteLine("Hello, World!");

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

            var rootCommand =
                new RootCommand("Utility per cancellare Exchange RabbitMQ basati su prefissi.")
                {
                    hostOption,
                    portOption,
                    userOption,
                    passOption,
                    vHostOption,
                    prefixesOption
                };

            rootCommand.SetAction(async (result, token) =>
            {
                var host = result.GetValue(hostOption);
                var port = result.GetValue<int>(portOption);
                var user = result.GetValue(userOption);
                var pass = result.GetValue(passOption);
                var prefixes = result.GetValue(prefixesOption)!;
                var vhost = result.GetValue(vHostOption)!;

                await CleanExchangesAsync(host, port, user, pass, vhost, prefixes);
            });

            var parseResult = rootCommand.Parse(args);

            if (parseResult.Tokens.Any())
            {
            }

            if (parseResult.Errors.Count <= 0)
                return await parseResult.InvokeAsync();

            // Gestione errori di parsing
            foreach (var error in parseResult.Errors)
            {
                ConsoleExt.WriteLine(ConsoleColor.Red, error.Message);
            }
            return 1; // Codice di errore

        }

        private static async Task CleanExchangesAsync(string? host, int port, string? username, string? password,
            string? vHost,
            string[]? prefixes)
        {

            var vHostSpecified = !string.IsNullOrEmpty(vHost);


            ConsoleExt.WriteLine(ConsoleColor.Cyan, $"Avvio pulizia su {host}...\n\r" +
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

                if (!response.IsSuccessStatusCode)
                {
                    ConsoleExt.WriteLine(ConsoleColor.Red, $"Errore API Management: {response.StatusCode} - {response.ReasonPhrase}");
                    return;
                }

                // 2. Parsing JSON con System.Text.Json
                var contentStream = await response.Content.ReadAsStreamAsync();

                // Usiamo JsonNode per un parsing dinamico simile a JArray di Newtonsoft
                var rootNode = await JsonNode.ParseAsync(contentStream);

                if (rootNode is not JsonArray exchangesArray)
                {
                    ConsoleExt.WriteLine(ConsoleColor.Red, "Formato risposta imprevisto (non è un array).");

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

                //var exchangesToDelete = allExchanges
                //    .Where(t => !string.IsNullOrEmpty(t.Name) &&
                //                prefixes!.Any(p => t.Name.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
                //    .ToList();

                if (exchangesToDelete.Count == 0)
                {
                    ConsoleExt.WriteLine(ConsoleColor.Red, "Nessun exchange trovato con i prefissi specificati.");
                    return;
                }

                Console.WriteLine($"Trovati {exchangesToDelete.Count} exchange da cancellare.");

                // 4. Cancellazione effettiva (RabbitMQ.Client)
                var groupedByVHost = exchangesToDelete.GroupBy(i => i.VHost);

                foreach (var group in groupedByVHost)
                {
                    var currentVHost = group.Key!;

                    try
                    {
                        Console.WriteLine($"Connessione al VHost: '{currentVHost}'...");

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
                                await channel.ExchangeDeleteAsync(exchangeToken.Name!);
                                ConsoleExt.WriteLine(ConsoleColor.Green, $"[ELIMINATO] {exchangeToken}");

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
                ConsoleExt.WriteLine(ConsoleColor.Red, $"Errore HTTP: {httpEx.Message}");
            }
            catch (Exception ex)
            {
                ConsoleExt.WriteLine(ConsoleColor.Red, $"Errore generale: {ex.Message}");
            }
        }
    }
}


/*


using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqCleaner;

internal class InfoToken
{
    public string? Name { get; set; }
    public string? VHost { get; set; }
    public override string ToString() => $"VHost: {VHost} - Name: {Name}";
}

internal class AppArguments
{
    public string Host { get; set; } = "localhost";
    public string User { get; set; } = "guest";
    public string Pass { get; set; } = "guest";
    public List<string> Prefixes { get; set; } = new();

    // Consideriamo validi gli argomenti solo se c'è almeno un prefisso
    public bool IsValid => Prefixes.Any();
}

class Program
{
    static async Task Main(string[] args)
    {
        // 1. Parsing iniziale da riga di comando
        var arguments = ParseArguments(args);

        // 2. Se mancano i dati essenziali (prefissi), chiediamo all'utente
        if (!arguments.IsValid)
        {
            CompletaParametriInterattivamente(arguments);
        }

        // 3. Se dopo l'input utente mancano ancora i prefissi, usciamo
        if (!arguments.IsValid)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("Nessun prefisso specificato. Operazione annullata.");
            Console.ResetColor();
            return;
        }

        // 4. Avvio logica
        await CleanExchangesAsync(arguments);
        
        // Mantiene la console aperta se eseguito a mano
        Console.WriteLine("\nPremere un tasto per chiudere...");
        Console.ReadKey();
    }

    static void CompletaParametriInterattivamente(AppArguments args)
    {
        Console.WriteLine("--- Parametri mancanti o avvio manuale ---");
        Console.WriteLine("Premi INVIO per accettare il valore di [default]");
        Console.WriteLine();

        // Host
        args.Host = LeggiInput("Host RabbitMQ", args.Host);

        // User
        args.User = LeggiInput("Username", args.User);

        // Pass
        args.Pass = LeggiInput("Password", args.Pass); // Nota: qui il testo sarà visibile

        // Prefissi
        Console.Write("Prefissi (separati da spazio o virgola): ");
        string? inputPrefixes = Console.ReadLine();

        if (!string.IsNullOrWhiteSpace(inputPrefixes))
        {
            // Divide per virgola o spazio e rimuove voci vuote
            var inputParts = inputPrefixes.Split(new[] { ' ', ',' }, StringSplitOptions.RemoveEmptyEntries);
            args.Prefixes.AddRange(inputParts);
        }
        Console.WriteLine("------------------------------------------\n");
    }

    static string LeggiInput(string etichetta, string valoreDefault)
    {
        Console.Write($"{etichetta} [{valoreDefault}]: ");
        string? input = Console.ReadLine();
        return string.IsNullOrWhiteSpace(input) ? valoreDefault : input;
    }

    static AppArguments ParseArguments(string[] args)
    {
        var result = new AppArguments();

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i].ToLower())
            {
                case "--host":
                    if (i + 1 < args.Length) result.Host = args[++i];
                    break;
                case "--user":
                    if (i + 1 < args.Length) result.User = args[++i];
                    break;
                case "--pass":
                    if (i + 1 < args.Length) result.Pass = args[++i];
                    break;
                case "--prefixes":
                    while (i + 1 < args.Length && !args[i + 1].StartsWith("-"))
                    {
                        result.Prefixes.Add(args[++i]);
                    }
                    break;
            }
        }
        return result;
    }

    static async Task CleanExchangesAsync(AppArguments args)
    {
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine($"Avvio pulizia su {args.Host}...");
        Console.WriteLine($"Prefissi target: {string.Join(", ", args.Prefixes)}");
        Console.ResetColor();

        using var httpClient = new HttpClient();
        // Timeout breve per evitare attese lunghe se l'host è sbagliato
        httpClient.Timeout = TimeSpan.FromSeconds(10); 

        try
        {
            httpClient.BaseAddress = new Uri($"http://{args.Host}:15672/api/");
        }
        catch (UriFormatException)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Formato Host non valido: {args.Host}");
            return;
        }
        
        var authString = Convert.ToBase64String(Encoding.ASCII.GetBytes($"{args.User}:{args.Pass}"));
        httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", authString);

        try
        {
            Console.WriteLine("Recupero lista exchange...");
            var response = await httpClient.GetAsync("exchanges");

            if (!response.IsSuccessStatusCode)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Errore API ({response.StatusCode}): Verifica credenziali o vhost permissions.");
                return;
            }

            var contentStream = await response.Content.ReadAsStreamAsync();
            var rootNode = await JsonNode.ParseAsync(contentStream);
            
            if (rootNode is not JsonArray exchangesArray)
            {
                Console.WriteLine("Risposta non valida dal server.");
                return;
            }

            var allExchanges = exchangesArray.Select(node => new InfoToken
            {
                Name = node?["name"]?.ToString(),
                VHost = node?["vhost"]?.ToString()
            });

            var exchangesToDelete = allExchanges
                .Where(t => !string.IsNullOrEmpty(t.Name) && 
                            args.Prefixes.Any(p => t.Name.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
                .ToList();

            if (exchangesToDelete.Count == 0)
            {
                Console.WriteLine("Nessun exchange trovato con i prefissi specificati.");
                return;
            }

            Console.WriteLine($"Trovati {exchangesToDelete.Count} exchange da cancellare.");

            var groupedByVHost = exchangesToDelete.GroupBy(i => i.VHost);

            foreach (var group in groupedByVHost)
            {
                string currentVHost = group.Key!;
                
                try
                {
                    Console.WriteLine($"Connessione al VHost: '{currentVHost}'...");

                    var factory = new ConnectionFactory
                    {
                        HostName = args.Host,
                        UserName = args.User,
                        Password = args.Pass,
                        VirtualHost = currentVHost
                    };

                    await using var connection = await factory.CreateConnectionAsync();
                    await using var channel = await connection.CreateChannelAsync();

                    foreach (var exchangeToken in group)
                    {
                        try 
                        {
                            await channel.ExchangeDeleteAsync(exchangeToken.Name!);
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"[ELIMINATO] {exchangeToken}");
                        }
                        catch (Exception ex)
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine($"[ERRORE DELETE] {exchangeToken.Name}: {ex.Message}");
                        }
                        finally
                        {
                            Console.ResetColor();
                        }
                    }
                }
                catch (BrokerUnreachableException)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Impossibile connettersi alla porta 5672 (AMQP) su {args.Host}.");
                    Console.ResetColor();
                }
            }
        }
        catch (HttpRequestException)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Impossibile contattare le API HTTP su {args.Host}:15672.");
            Console.ResetColor();
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Errore inatteso: {ex.Message}");
            Console.ResetColor();
        }
    }
}




*/
