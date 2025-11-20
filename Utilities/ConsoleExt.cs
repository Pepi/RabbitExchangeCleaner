namespace RabbitExchangeCleaner.Utilities
{
    public static class ConsoleExt
    {
        public static void WriteLine(ConsoleColor foregroundColor, string? value)
        {
            var previousColor = Console.ForegroundColor;
            Console.ForegroundColor = foregroundColor;
            Console.WriteLine(value);
            Console.ForegroundColor = previousColor;
        }
    }
}