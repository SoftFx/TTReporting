using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;

using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

internal static class Program
{
    public static int Main(string[] args)
    {
        try
        {
            // Root проекта Swaps = текущая рабочая директория (важно запускать из корня Swaps)
            var root = Directory.GetCurrentDirectory();

            // Пути "как в проекте": все лежит в configDocker
            var configYaml = Path.Combine(root, "configDocker", "config.yaml");
            var mappingCsv = Path.Combine(root, "configDocker", "mapping.csv");
            var libsDir = Path.Combine(root, "configDocker", "libs");

            // Базовая диагностика (чтобы сразу видеть, откуда запускается и какие файлы читаем)
            Console.WriteLine("------------------------------------------------");
            Console.WriteLine("TtsSwapsFetcher started");
            Console.WriteLine("root       = " + root);
            Console.WriteLine("configYaml = " + configYaml);
            Console.WriteLine("mappingCsv = " + mappingCsv);
            Console.WriteLine("libsDir    = " + libsDir);
            Console.WriteLine("------------------------------------------------");

            // Жесткая проверка входных файлов/папок — если чего-то нет, лучше упасть сразу
            if (!File.Exists(configYaml))
                throw new FileNotFoundException("config.yaml not found", configYaml);

            if (!File.Exists(mappingCsv))
                throw new FileNotFoundException("mapping.csv not found", mappingCsv);

            if (!Directory.Exists(libsDir))
                throw new DirectoryNotFoundException("libs dir not found: " + libsDir);

            // Десериализатор YAML:
            // - UnderscoredNamingConvention: под snake_case (today_dir, symbols_filter, tts_configs)
            // - IgnoreUnmatchedProperties: игнорируем лишние секции yaml, чтобы не падать
            var deserializer = new DeserializerBuilder()
                .WithNamingConvention(UnderscoredNamingConvention.Instance)
                .IgnoreUnmatchedProperties()
                .Build();

            // Читаем YAML и превращаем в RootConfig (модели ниже)
            var yamlText = File.ReadAllText(configYaml);
            var cfg = deserializer.Deserialize<RootConfig>(yamlText)
                      ?? throw new InvalidOperationException("Failed to deserialize config.yaml");

            // effectiveRoot:
            // - если в yaml root не задан, используем текущий root (Directory.GetCurrentDirectory)
            // - если root задан относительным путем — делаем абсолютный относительно текущего root
            var effectiveRoot = string.IsNullOrWhiteSpace(cfg.Root) ? root : cfg.Root!.Trim();
            if (!Path.IsPathRooted(effectiveRoot))
                effectiveRoot = Path.GetFullPath(Path.Combine(root, effectiveRoot));

            // todayDir:
            // - если в yaml paths.today_dir не задан → дефолт dataDocker/today
            // - если задан относительный → тоже делаем абсолютным от effectiveRoot
            var todayDir = cfg.Paths?.TodayDir;
            if (string.IsNullOrWhiteSpace(todayDir))
                todayDir = Path.Combine(effectiveRoot, "dataDocker", "today");
            else if (!Path.IsPathRooted(todayDir))
                todayDir = Path.GetFullPath(Path.Combine(effectiveRoot, todayDir));

            // Берем первую конфигурацию TTS из tts_configs[0]
            var tts = cfg.TtsConfigs?.FirstOrDefault()
                      ?? throw new InvalidOperationException("config.yaml: tts_configs[0] not found");

            // Минимальная валидация
            if (string.IsNullOrWhiteSpace(tts.Server))
                throw new InvalidOperationException("config.yaml: tts_configs[0].server is empty");
            if (string.IsNullOrWhiteSpace(tts.Login))
                throw new InvalidOperationException("config.yaml: tts_configs[0].login is empty");

            // Куда сохраняем:
            // dataDocker/today/DD-MM-YYYY/tts_swaps_current.csv
            var todayFolder = Path.Combine(todayDir, DateTime.Now.ToString("dd-MM-yyyy"));
            Directory.CreateDirectory(todayFolder);

            var outputCsv = Path.Combine(todayFolder, "tts_swaps_current.csv");

            // Читаем mapping.csv и вытаскиваем уникальные TTS symbols
            var mapping = CsvMapping.Load(mappingCsv);

            var ttsSymbols = mapping
                .Select(r => r.Tts)
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Select(s => s.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            // symbols_filter из config.yaml:
            // пример: "*,!#*" → разрешить все, кроме начинающихся с #
            // если пусто → фильтр пропускает все
            var filters = GlobFilters.Parse(tts.SymbolsFilter ?? "");
            ttsSymbols = ttsSymbols.Where(sym => filters.IsAllowed(sym)).ToList();

            Console.WriteLine("[INFO] server: " + tts.Server);
            Console.WriteLine("[INFO] login:  " + tts.Login);
            Console.WriteLine("[INFO] filter: " + (tts.SymbolsFilter ?? ""));
            Console.WriteLine("[INFO] symbols after mapping+filter: " + ttsSymbols.Count);
            Console.WriteLine("[INFO] out:    " + outputCsv);

            // libs:
            // - если есть configDocker/libs/tts → используем его
            // - иначе используем configDocker/libs
            // Это удобно, потому что в пайплайне uploader у тебя уже работает с libs/tts
            var libsTtsDir = Path.Combine(libsDir, "tts");
            if (Directory.Exists(libsTtsDir))
            {
                Console.WriteLine("[INFO] libs:    " + libsTtsDir + " (auto: libs/tts)");
            }
            else
            {
                libsTtsDir = libsDir;
                Console.WriteLine("[INFO] libs:    " + libsTtsDir);
            }

            // Создаем fetcher, который:
            // - грузит dll из libs через reflection
            // - находит TickTraderManagerModel
            // - Connect()
            // - по каждому symbol вытаскивает swap_long / swap_short
            var fetcher = new TickTraderManApiFetcher(libsTtsDir);

            var rows = fetcher.FetchSwaps(
                tts.Server.Trim(),
                tts.Login.Trim(),
                (tts.Password ?? "").Trim(),
                ttsSymbols
            );

            // Сохраняем результат в CSV
            WriteCsv(outputCsv, rows);

            Console.WriteLine("[OK] Saved: " + outputCsv + " | rows: " + rows.Count);
            return 0;
        }
        catch (Exception ex)
        {
            // Любая ошибка → exit code 1 (Python-оркестратор по returncode поймёт что шаг упал)
            Console.WriteLine("[ERROR] " + ex);
            return 1;
        }
    }

    // Пишем CSV в стандарте:
    // tts,swap_long,swap_short
    // <symbol>,<decimal>,<decimal>
    // decimal в InvariantCulture (точка как разделитель)
    private static void WriteCsv(string path, List<TtsSwapRow> rows)
    {
        var sb = new StringBuilder();
        sb.AppendLine("tts,swap_long,swap_short");

        foreach (var r in rows.OrderBy(x => x.Symbol, StringComparer.OrdinalIgnoreCase))
        {
            sb.Append(r.Symbol).Append(',');
            sb.Append(r.SwapLong.ToString(CultureInfo.InvariantCulture)).Append(',');
            sb.Append(r.SwapShort.ToString(CultureInfo.InvariantCulture));
            sb.AppendLine();
        }

        File.WriteAllText(path, sb.ToString(), new UTF8Encoding(false));
    }
}

// Модели YAML под UnderscoredNamingConvention:
// root
// paths:
//   today_dir
// tts_configs:
//   - server
//     login
//     password
//     symbols_filter
internal sealed class RootConfig
{
    public string? Root { get; set; }
    public PathsConfig? Paths { get; set; }
    public List<TtsConfig>? TtsConfigs { get; set; }  // tts_configs
}

internal sealed class PathsConfig
{
    public string? TodayDir { get; set; } // today_dir
}

internal sealed class TtsConfig
{
    public string? Name { get; set; }
    public string Server { get; set; } = "";
    public string Login { get; set; } = "";
    public string? Password { get; set; }
    public string? SymbolsFilter { get; set; } // symbols_filter
}

// Парсер mapping.csv
// Ожидаемые колонки: mt4, mt5, tts, lp, source (но реально достаточно tts)
internal sealed class CsvMapping
{
    public string Mt4 { get; set; } = "";
    public string Mt5 { get; set; } = "";
    public string Tts { get; set; } = "";
    public string Lp { get; set; } = "";
    public string Source { get; set; } = "";

    public static List<CsvMapping> Load(string path)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException("mapping.csv not found", path);

        var lines = File.ReadAllLines(path);
        if (lines.Length < 2) return new List<CsvMapping>();

        // Автоопределение разделителя: ; / tab / ,
        char sep = DetectSeparator(lines[0]);
        var headers = SplitCsvLine(lines[0], sep).Select(h => h.Trim()).ToList();

        int idxMt4 = IndexOf(headers, "mt4");
        int idxMt5 = IndexOf(headers, "mt5");
        int idxTts = IndexOf(headers, "tts");
        int idxLp = IndexOf(headers, "lp");
        int idxSource = IndexOf(headers, "source");

        if (idxTts < 0)
            throw new InvalidOperationException("mapping.csv must contain column 'tts'");

        var rows = new List<CsvMapping>();
        for (int i = 1; i < lines.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i])) continue;
            var cols = SplitCsvLine(lines[i], sep);

            rows.Add(new CsvMapping
            {
                Mt4 = Get(cols, idxMt4),
                Mt5 = Get(cols, idxMt5),
                Tts = Get(cols, idxTts),
                Lp = Get(cols, idxLp),
                Source = Get(cols, idxSource),
            });
        }

        return rows;
    }

    private static int IndexOf(List<string> headers, string name)
        => headers.FindIndex(h => string.Equals(h, name, StringComparison.OrdinalIgnoreCase));

    private static string Get(List<string> cols, int idx)
        => (idx >= 0 && idx < cols.Count) ? (cols[idx] ?? "").Trim() : "";

    private static char DetectSeparator(string line)
    {
        if (line.IndexOf(';') >= 0 && line.IndexOf(',') < 0) return ';';
        if (line.IndexOf('\t') >= 0) return '\t';
        return ',';
    }

    // Простой CSV split с поддержкой кавычек (минимально достаточно для mapping.csv)
    private static List<string> SplitCsvLine(string line, char sep)
    {
        var res = new List<string>();
        var sb = new StringBuilder();
        bool inQuotes = false;

        for (int i = 0; i < line.Length; i++)
        {
            char c = line[i];

            if (c == '"')
            {
                inQuotes = !inQuotes;
                continue;
            }

            if (c == sep && !inQuotes)
            {
                res.Add(sb.ToString());
                sb.Clear();
                continue;
            }

            sb.Append(c);
        }

        res.Add(sb.ToString());
        return res;
    }
}

// symbols_filter="*,!#*"
// - include: все что без "!" (если include пустой → разрешаем все)
// - exclude: все что с "!"
internal sealed class GlobFilters
{
    private readonly List<Regex> _exclude = new();
    private readonly List<Regex> _include = new();

    public static GlobFilters Parse(string raw)
    {
        var f = new GlobFilters();
        if (string.IsNullOrWhiteSpace(raw)) return f;

        foreach (var part in raw
            .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(x => x.Trim()))
        {
            if (part.Length == 0) continue;

            if (part.StartsWith("!"))
                f._exclude.Add(GlobToRegex(part.Substring(1)));
            else
                f._include.Add(GlobToRegex(part));
        }

        return f;
    }

    public bool IsAllowed(string symbol)
    {
        // Сначала исключения
        if (_exclude.Any(r => r.IsMatch(symbol)))
            return false;

        // Если include не задан, значит разрешаем все
        if (_include.Count == 0)
            return true;

        // Иначе разрешаем только то, что совпало с include
        return _include.Any(r => r.IsMatch(symbol));
    }

    private static Regex GlobToRegex(string glob)
    {
        string pattern = "^" + Regex.Escape(glob)
            .Replace("\\*", ".*")
            .Replace("\\?", ".") + "$";
        return new Regex(pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }
}

// Модель результата (одна строка в tts_swaps_current.csv)
internal sealed class TtsSwapRow
{
    public string Symbol { get; }
    public decimal SwapLong { get; }
    public decimal SwapShort { get; }

    public TtsSwapRow(string symbol, decimal swapLong, decimal swapShort)
    {
        Symbol = symbol;
        SwapLong = swapLong;
        SwapShort = swapShort;
    }
}

// Fetcher TickTrader Manager API через reflection
internal sealed class TickTraderManApiFetcher
{
    private readonly string _libsDir;

    public TickTraderManApiFetcher(string libsDir)
    {
        _libsDir = libsDir;
        if (!Directory.Exists(_libsDir))
            throw new DirectoryNotFoundException("libs dir not found: " + _libsDir);
    }

    public List<TtsSwapRow> FetchSwaps(string server, string login, string password, List<string> symbols)
    {
        // Чтобы зависимости из libs подтягивались автоматически при загрузке dll
        AppDomain.CurrentDomain.AssemblyResolve += ResolveFromLibs;

        // Загружаем все dll из libs/*.dll
        var dlls = Directory.GetFiles(_libsDir, "*.dll", SearchOption.TopDirectoryOnly);
        var loadedAssemblies = new List<Assembly>();

        foreach (var dll in dlls)
        {
            var asm = TryLoad(dll);
            if (asm != null) loadedAssemblies.Add(asm);
        }

        if (loadedAssemblies.Count == 0)
            throw new InvalidOperationException("No assemblies loaded from libs. Check libs/*.dll");

        // Собираем все типы со всех dll
        var allTypes = new List<Type>();
        foreach (var a in loadedAssemblies)
            allTypes.AddRange(SafeGetTypes(a));

        // Ищем главный тип TickTrader manager
        var managerType = allTypes.FirstOrDefault(t =>
            string.Equals(t.FullName, "TickTrader.Manager.Model.TickTraderManagerModel", StringComparison.Ordinal));

        if (managerType == null)
            throw new InvalidOperationException("Could not locate TickTrader Manager type: TickTrader.Manager.Model.TickTraderManagerModel");

        // Создаем инстанс manager
        object manager = Activator.CreateInstance(managerType)
            ?? throw new InvalidOperationException("Failed to create manager instance.");

        // Подключаемся (Connect может быть перегружен: long login или string login)
        InvokeConnect(manager, server, login, password);

        var result = new List<TtsSwapRow>(symbols.Count);

        // По каждому symbol пытаемся достать config объекта и из него вытянуть swapLong/swapShort
        foreach (var sym in symbols)
        {
            decimal sl = 0m, ss = 0m;

            // В разных версиях API может быть метод GetSymbolConfig или RequestSymbol
            var cfgObj =
                TryInvoke1(manager, "GetSymbolConfig", sym) ??
                TryInvoke1(manager, "RequestSymbol", sym);

            // SwapExtractor проходит по свойствам объекта и ищет подходящие поля swap long/short
            if (cfgObj != null)
                (sl, ss) = SwapExtractor.Extract(cfgObj);

            result.Add(new TtsSwapRow(sym, sl, ss));
        }

        // Корректно закрываем соединение (на разных версиях может называться по-разному)
        TryInvoke0(manager, "Disconnect", "Close", "Logout");
        return result;
    }

    // AssemblyResolve: если при загрузке dll нужна зависимость — ищем ее в libsDir
    private Assembly? ResolveFromLibs(object sender, ResolveEventArgs args)
    {
        try
        {
            var name = new AssemblyName(args.Name).Name;
            if (string.IsNullOrWhiteSpace(name)) return null;

            var candidate = Path.Combine(_libsDir, name + ".dll");
            if (File.Exists(candidate))
                return Assembly.LoadFrom(candidate);
        }
        catch { }
        return null;
    }

    // Безопасная загрузка dll (если не грузится — просто пропускаем)
    private static Assembly? TryLoad(string dllPath)
    {
        try { return Assembly.LoadFrom(dllPath); }
        catch { return null; }
    }

    // Безопасное получение типов, чтобы ReflectionTypeLoadException не убил процесс
    private static List<Type> SafeGetTypes(Assembly asm)
    {
        try { return asm.GetTypes().Where(t => t != null).ToList()!; }
        catch (ReflectionTypeLoadException rtle)
        {
            return rtle.Types.Where(t => t != null).ToList()!;
        }
        catch
        {
            return new List<Type>();
        }
    }

    // Находим подходящую перегрузку Connect и вызываем ее
    private static void InvokeConnect(object manager, string server, string login, string password)
    {
        var t = manager.GetType();
        var methods = t.GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => string.Equals(m.Name, "Connect", StringComparison.OrdinalIgnoreCase))
            .ToList();

        foreach (var m in methods)
        {
            var ps = m.GetParameters();

            try
            {
                // Connect(string, long, string)
                if (ps.Length == 3 &&
                    ps[0].ParameterType == typeof(string) &&
                    (ps[1].ParameterType == typeof(long) || ps[1].ParameterType == typeof(Int64)) &&
                    ps[2].ParameterType == typeof(string))
                {
                    if (!long.TryParse(login, out var loginLong))
                        throw new InvalidOperationException("Login '" + login + "' is not a valid Int64");

                    m.Invoke(manager, new object[] { server, loginLong, password });
                    return;
                }

                // Connect(string, string, string)
                if (ps.Length == 3 &&
                    ps[0].ParameterType == typeof(string) &&
                    ps[1].ParameterType == typeof(string) &&
                    ps[2].ParameterType == typeof(string))
                {
                    m.Invoke(manager, new object[] { server, login, password });
                    return;
                }
            }
            catch (TargetInvocationException tie)
            {
                var msg = tie.InnerException?.Message ?? tie.Message;
                throw new InvalidOperationException("Connect failed: " + msg, tie.InnerException ?? tie);
            }
        }

        throw new InvalidOperationException("Could not invoke Connect().");
    }

    // Пытаемся вызвать метод с 1 аргументом string и вернуть результат (или null)
    private static object? TryInvoke1(object obj, string methodName, string arg)
    {
        var m = obj.GetType().GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault(x =>
                string.Equals(x.Name, methodName, StringComparison.OrdinalIgnoreCase) &&
                x.GetParameters().Length == 1 &&
                x.GetParameters()[0].ParameterType == typeof(string));

        if (m == null) return null;

        try
        {
            var res = m.Invoke(obj, new object[] { arg });

            // Если вернули Task<T> — забираем Result
            if (res != null && res.GetType().Name.StartsWith("Task", StringComparison.OrdinalIgnoreCase))
            {
                var rp = res.GetType().GetProperty("Result");
                if (rp != null) res = rp.GetValue(res, null);
            }

            return res;
        }
        catch
        {
            return null;
        }
    }

    // Закрываем соединение (вызов первого найденного метода без аргументов)
    private static void TryInvoke0(object obj, params string[] methodNames)
    {
        foreach (var name in methodNames)
        {
            var m = obj.GetType().GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(x =>
                    string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase) &&
                    x.GetParameters().Length == 0);

            if (m == null) continue;
            try { m.Invoke(obj, null); } catch { }
            return;
        }
    }
}

// Extractor swap values из неизвестного объекта конфигурации symbol (разные версии API дают разные модели)
internal static class SwapExtractor
{
    // Кандидатное поле swap (значение + “насколько оно похоже на нужное”)
    private sealed class Candidate
    {
        public decimal Value;
        public int Score;
        public bool NonZero;
    }

    // Главный метод: возвращает (swapLong, swapShort)
    public static (decimal swapLong, decimal swapShort) Extract(object symbolConfig)
    {
        // visited нужен, чтобы не уйти в циклы по объектам
        var visited = new HashSet<object>(ReferenceEqualityComparer.Instance);

        // собираем кандидаты long/short
        var longCands = new List<Candidate>();
        var shortCands = new List<Candidate>();

        // рекурсивно обходим object graph до depth=3
        Visit(symbolConfig, 0, visited, longCands, shortCands);

        // выбираем лучшие значения по score и абсолютной величине
        var bestLong = PickBest(longCands);
        var bestShort = PickBest(shortCands);

        return (bestLong, bestShort);
    }

    // Выбор лучшего кандидата:
    // - если есть non-zero кандидаты → выбираем среди них
    // - иначе выбираем среди всех
    private static decimal PickBest(List<Candidate> cands)
    {
        if (cands.Count == 0) return 0m;

        var nonZero = cands.Where(c => c.NonZero).ToList();
        var list = nonZero.Count > 0 ? nonZero : cands;

        return list
            .OrderByDescending(c => c.Score)
            .ThenByDescending(c => Math.Abs(c.Value))
            .First()
            .Value;
    }

    // Рекурсивный обход свойств объекта
    private static void Visit(
        object obj,
        int depth,
        HashSet<object> visited,
        List<Candidate> longCands,
        List<Candidate> shortCands)
    {
        if (obj == null) return;
        if (depth > 3) return;          // ограничиваем глубину, чтобы не уходить в сложные графы
        if (!visited.Add(obj)) return;  // защита от циклов

        var t = obj.GetType();

        foreach (var p in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!p.CanRead) continue;

            object? v = null;
            try { v = p.GetValue(obj, null); } catch { continue; }
            if (v == null) continue;

            var name = p.Name ?? "";

            // Если значение можно превратить в decimal — это кандидат
            if (TryToDecimal(v, out var dec))
            {
                var n = name.ToLowerInvariant();

                // выкидываем трипл-свопы/день недели и т.п.
                if (n.Contains("triple") || n.Contains("day") || n.Contains("wednesday")) continue;

                // интересуют только свойства, связанные со swap/financing
                if (!n.Contains("swap") && !n.Contains("financ")) continue;

                // скоринг по ключевым словам
                int baseScore = 0;
                baseScore += n.Contains("swap") ? 6 : 0;
                baseScore += n.Contains("financ") ? 6 : 0;
                baseScore += n.Contains("size") ? 4 : 0;
                baseScore += n.Contains("points") ? 2 : 0;
                baseScore += n.Contains("rate") ? 2 : 0;

                // для long: усиливаем long/buy, ослабляем short/sell
                int scoreLong = baseScore;
                if (n.Contains("long") || n.Contains("buy")) scoreLong += 6;
                if (n.Contains("short") || n.Contains("sell")) scoreLong -= 3;

                // для short: усиливаем short/sell, ослабляем long/buy
                int scoreShort = baseScore;
                if (n.Contains("short") || n.Contains("sell")) scoreShort += 6;
                if (n.Contains("long") || n.Contains("buy")) scoreShort -= 3;

                if (scoreLong > 0)
                    longCands.Add(new Candidate { Value = dec, Score = scoreLong, NonZero = dec != 0m });

                if (scoreShort > 0)
                    shortCands.Add(new Candidate { Value = dec, Score = scoreShort, NonZero = dec != 0m });

                continue;
            }

            // Пропускаем строки, enum и некоторые коллекции (чтобы не уходить в огромные графы)
            if (v is string) continue;
            if (v.GetType().IsEnum) continue;

            // Коллекции обычно не нужны, и могут быть тяжелыми
            if (v is System.Collections.IEnumerable && v is not IDictionary<string, object>)
                continue;

            // ValueType без свойств не имеет смысла углублять
            if (v.GetType().IsValueType && v.GetType().GetProperties().Length == 0)
                continue;

            // Углубляемся в nested object
            Visit(v, depth + 1, visited, longCands, shortCands);
        }
    }

    // Пробуем привести объект к decimal максимально надежно (разные типы возможны)
    private static bool TryToDecimal(object v, out decimal dec)
    {
        switch (v)
        {
            case decimal d: dec = d; return true;
            case double db: dec = (decimal)db; return true;
            case float f: dec = (decimal)f; return true;
            case int i: dec = i; return true;
            case long l: dec = l; return true;
            case short s: dec = s; return true;
            case byte b: dec = b; return true;
        }

        if (decimal.TryParse(v.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out dec))
            return true;

        if (decimal.TryParse(v.ToString(), NumberStyles.Any, CultureInfo.CurrentCulture, out dec))
            return true;

        dec = 0m;
        return false;
    }

    // Reference equality comparer нужен для HashSet visited (чтобы сравнивать по ссылке, а не по Equals())
    private sealed class ReferenceEqualityComparer : IEqualityComparer<object>
    {
        public static readonly ReferenceEqualityComparer Instance = new ReferenceEqualityComparer();
        public new bool Equals(object? x, object? y) => ReferenceEquals(x, y);
        public int GetHashCode(object obj) => System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj);
    }
}
