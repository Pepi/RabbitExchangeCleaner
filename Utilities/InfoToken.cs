namespace RabbitExchangeCleaner.Utilities
{
    internal class InfoToken
    {
        public string? Name { get; set; }
        public string? VHost { get; set; }

        public override string ToString() => $"VHost: {VHost} - Name: {Name}";
    }
}