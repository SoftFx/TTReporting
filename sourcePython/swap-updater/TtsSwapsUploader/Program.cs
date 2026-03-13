using System;
using System.IO;
using System.Text;
using System.Globalization;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;

internal class Program
{
    static int Main(string[] args)
    {
        FileLogger? logger = null;

        try
        {
            // Разбираем аргументы командной строки вида:
            // --config ... --libs ... --mapping ... --swaps ...
            // --dryRun true/false --onlyIfDifferent true/false
            var parsed = Args.Parse(args);

            // Обязательные параметры
            string configPath = Require(parsed, "--config");
            string libsDir = Require(parsed, "--libs");
            string mappingPathArg = Require(parsed, "--mapping");

            // Необязательные параметры
            string? swapsPathArg = GetOpt(parsed, "--swaps");
            bool dryRun = ParseBool(GetOpt(parsed, "--dryRun"), false);
            bool onlyIfDifferent = ParseBool(GetOpt(parsed, "--onlyIfDifferent"), true);

            // Определяем “корень Swaps”:
            // если config лежит внутри папки configDocker, тогда корень = родитель configDocker
            // (это нужно, чтобы одинаково работали относительные пути как при ручном запуске)
            string swapsRoot = ResolveSwapsRootFromConfig(configPath);

            // Лог пишем в dataDocker/log/Push-YYYY-MM-DD.log
            string logPath = Path.Combine(
                swapsRoot,
                "dataDocker",
                "log",
                "Push-" + DateTime.Now.ToString("yyyy-MM-dd") + ".log"
            );
            logger = new FileLogger(logPath);

            logger.Info("START PushSwaps | dryRun=" + dryRun + " onlyIfDifferent=" + onlyIfDifferent);
            logger.Info("paths | config=" + configPath + " mapping=" + mappingPathArg + " libs=" + libsDir + " swapsRoot=" + swapsRoot);
            logger.Info("log | file=" + logPath);

            // Читаем config.yaml.
            // Здесь не используем полноценную yaml-библиотеку, а читаем минимально то, что нам нужно:
            // - первый tts_configs (server/login/password)
            // - tts_uploader.demo_missing_symbols.<server> (опционально)
            SimpleYaml cfg = SimpleYaml.Load(configPath);
            TtsConfig tts = cfg.ReadTtsConfigFirst();

            logger.Info("tts | server=" + tts.Server + " login=" + tts.Login);

            // Для demo-сервера: иногда в YAML заранее прописываем список символов, которых нет на демо.
            // Их надо пропускать, чтобы не ловить ошибки при пуше.
            HashSet<string> demoMissingSymbols = cfg.GetDemoMissingSymbolsForServer(tts.Server);
            if (demoMissingSymbols.Count > 0)
                logger.Info("demo_missing_symbols | server=" + tts.Server + " count=" + demoMissingSymbols.Count);

            // mapping.csv может быть передан относительным путём — приводим его к абсолютному от swapsRoot
            string mappingPath = ResolvePath(mappingPathArg, swapsRoot);
            if (!File.Exists(mappingPath))
                throw new FileNotFoundException("mapping.csv not found", mappingPath);

            // swaps.csv можно указать параметром --swaps
            // если не указали — пытаемся найти автоматически в dataDocker/today/...
            string swapsPath = ResolveSwapsCsvPath(swapsPathArg, swapsRoot);
            if (!File.Exists(swapsPath))
                throw new FileNotFoundException("swaps.csv not found", swapsPath);

            logger.Info("mapping | file=" + mappingPath);
            logger.Info("swaps | file=" + swapsPath);

            // Загружаем mapping.csv и строим индекс вида (lp + source) -> tts_symbol
            List<MappingRow> mapping = MappingCsv.Load(mappingPath);
            MappingIndex mapIndex = MappingIndex.Build(mapping);

            // Загружаем рассчитанные свопы из swaps.csv (Symbol, NewSwapLong, NewSwapShort, Source, Group)
            List<SwapSourceRow> swaps = SwapsCsv.Load(swapsPath);
            if (swaps.Count == 0)
            {
                logger.Warn("swaps.csv contains 0 rows. Nothing to upload.");
                logger.Info("END PushSwaps | nothing to upload (0 rows)");
                return 0;
            }

            // Готовим список строк к загрузке (уже с найденным TTS-символом)
            List<SwapUploadRow> uploadRows = new List<SwapUploadRow>(swaps.Count);
            int mappingErrors = 0;

            for (int i = 0; i < swaps.Count; i++)
            {
                var s = swaps[i];

                // В swaps.csv обязательно должен быть Source (провайдер / LP),
                // иначе мы не сможем правильно сопоставить с mapping.csv
                if (string.IsNullOrWhiteSpace(s.Source))
                {
                    mappingErrors++;
                    logger.Warn("ERROR mapping: missing source in swaps.csv for lp='" + s.LpSymbol + "'");
                    continue;
                }

                // Нормализуем названия источников (velocitytrade -> velocity, is-prime -> isprime, ...)
                string normalizedSource = SourceNormalizer.Normalize(s.Source);

                // Пытаемся найти TTS-символ по связке (lp_symbol + source)
                string errMsg;
                string? ttsSymbol = mapIndex.ResolveTtsSymbolStrict(s.LpSymbol, normalizedSource, out errMsg);
                if (ttsSymbol == null)
                {
                    mappingErrors++;
                    logger.Warn(
                        "ERROR mapping: LP='" + s.LpSymbol + "' source='" + normalizedSource +
                        "' (raw='" + s.Source + "') => " + errMsg
                    );
                    continue;
                }

                // Если маппинг найден — добавляем строку для аплоада
                uploadRows.Add(new SwapUploadRow(
                    ttsSymbol,
                    s.LpSymbol,
                    normalizedSource,
                    s.Source,
                    s.NewSwapLong,
                    s.NewSwapShort,
                    s.Group
                ));
            }

            logger.Info("rows | swaps=" + swaps.Count + " upload=" + uploadRows.Count + " mapping_errors=" + mappingErrors);

            // Если есть ошибки маппинга — сознательно останавливаемся и не пушим ничего,
            // чтобы не было частичных/кривых обновлений.
            if (mappingErrors > 0)
            {
                logger.Warn("STOP due to mapping errors. No upload performed.");
                return 3;
            }

            // Инициализируем uploader, который будет грузить dll из libs и дергать Manager API через reflection
            TickTraderSwapsUploader uploader = new TickTraderSwapsUploader(libsDir);

            // Запускаем аплоад
            UploadResult result = uploader.UploadSwaps(
                server: tts.Server,
                login: tts.Login,
                password: tts.Password,
                swaps: uploadRows,
                dryRun: dryRun,
                onlyIfDifferent: onlyIfDifferent,
                logger: logger,
                demoMissingSymbols: demoMissingSymbols
            );

            logger.Info("END PushSwaps | total=" + result.Total + " changed=" + result.Changed + " skipped=" + result.Skipped + " failed=" + result.Failed);

            // В консоль выводим короткий итог (чтобы удобно видеть в dotnet run)
            Console.WriteLine("[OK] Done. total=" + result.Total + " changed=" + result.Changed + " skipped=" + result.Skipped + " failed=" + result.Failed);

            // Если были ошибки по отдельным символам — считаем это “частичный fail”
            return (result.Errors.Count > 0) ? 2 : 0;
        }
        catch (Exception ex)
        {
            // При фатальной ошибке — выводим в консоль и пишем в лог (если логгер уже успели создать)
            Console.WriteLine("[FATAL] " + ex.Message);
            try { logger?.Warn("FATAL " + ex); } catch { }
            return 1;
        }
    }

    // Args / paths

    static string Require(Dictionary<string, string> args, string key)
    {
        // Достаём обязательный аргумент --key VALUE
        if (args.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v))
            return v;
        throw new ArgumentException("Missing required argument: " + key);
    }

    static string? GetOpt(Dictionary<string, string> args, string key)
    {
        // Достаём необязательный аргумент. Если его нет — возвращаем null.
        if (args.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v))
            return v;
        return null;
    }

    static bool ParseBool(string? v, bool defaultValue)
    {
        // Нормальный парсер bool:
        // true/false, 1/0, пусто -> default
        if (string.IsNullOrWhiteSpace(v)) return defaultValue;
        v = v.Trim();

        if (bool.TryParse(v, out var b)) return b;
        if (v == "1") return true;
        if (v == "0") return false;

        return defaultValue;
    }

    static string ResolveSwapsRootFromConfig(string configPath)
    {
        // Определяем директорию config.yaml
        string? cfgDir = Path.GetDirectoryName(configPath);
        if (string.IsNullOrWhiteSpace(cfgDir))
            throw new InvalidOperationException("Cannot determine config directory from path: " + configPath);

        var di = new DirectoryInfo(cfgDir);
        if (!di.Exists)
            throw new DirectoryNotFoundException("Config directory not found: " + cfgDir);

        // Если конфиг лежит в configDocker, то корень проекта — на уровень выше
        if (string.Equals(di.Name, "configDocker", StringComparison.OrdinalIgnoreCase))
        {
            var parent = di.Parent;
            if (parent == null)
                throw new InvalidOperationException("configDocker has no parent dir, cannot resolve Swaps root.");
            return parent.FullName;
        }

        // Иначе считаем, что “корень” = директория, где лежит конфиг
        return di.FullName;
    }

    static string ResolvePath(string pathOrRelative, string swapsRoot)
    {
        // Если путь уже абсолютный — возвращаем как есть.
        // Если относительный — считаем, что он относительно swapsRoot.
        if (Path.IsPathRooted(pathOrRelative))
            return pathOrRelative;

        return Path.GetFullPath(Path.Combine(swapsRoot, pathOrRelative));
    }

    static string ResolveSwapsCsvPath(string? swapsPathArg, string swapsRoot)
    {
        // Если передано --swaps, используем его
        if (!string.IsNullOrWhiteSpace(swapsPathArg))
        {
            string p = ResolvePath(swapsPathArg, swapsRoot);
            if (!File.Exists(p))
                throw new FileNotFoundException("swaps.csv not found", p);
            return p;
        }

        // 1) Пробуем “прямой путь” (на всякий случай)
        // dataDocker/today/swaps.csv
        string direct = Path.Combine(swapsRoot, "dataDocker", "today", "swaps.csv");
        if (File.Exists(direct))
            return direct;

        // 2) Иначе ищем самый свежий DD-MM-YYYY каталог в dataDocker/today
        string todayRoot = Path.Combine(swapsRoot, "dataDocker", "today");
        if (!Directory.Exists(todayRoot))
            throw new DirectoryNotFoundException("today dir not found: " + todayRoot);

        string[] dirs = Directory.GetDirectories(todayRoot);
        var dated = new List<Tuple<string, DateTime>>();

        for (int i = 0; i < dirs.Length; i++)
        {
            string name = Path.GetFileName(dirs[i]);
            if (DateTime.TryParseExact(name, "dd-MM-yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                dated.Add(Tuple.Create(dirs[i], dt));
        }

        // Сортируем от самого нового к самому старому
        dated.Sort((a, b) => b.Item2.CompareTo(a.Item2));

        // Берём первый каталог, где реально есть swaps.csv
        for (int i = 0; i < dated.Count; i++)
        {
            string candidate = Path.Combine(dated[i].Item1, "swaps.csv");
            if (File.Exists(candidate))
                return candidate;
        }

        throw new FileNotFoundException("Cannot locate swaps.csv inside: " + todayRoot);
    }
}

// Нормализация имен источников (чтобы velocitytrade == velocity, и т.п.)
internal static class SourceNormalizer
{
    public static string Normalize(string raw)
    {
        raw = (raw ?? "").Trim();
        string low = raw.ToLowerInvariant();

        if (low == "velocitytrade") return "velocity";
        if (low == "velocity") return "velocity";

        if (low == "isprime") return "isprime";
        if (low == "is_prime") return "isprime";
        if (low == "is-prime") return "isprime";

        // Если нет спец-правила — оставляем как есть
        return raw;
    }
}

// Простой файловый логгер.
// Нам важно иметь лог, даже если всё запускается как console tool.
internal sealed class FileLogger
{
    private readonly string _logPath;
    private readonly object _lock = new object();

    public FileLogger(string logPath)
    {
        _logPath = logPath;

        // Создаём папку для лога, если её ещё нет
        string? dir = Path.GetDirectoryName(_logPath);
        if (!string.IsNullOrWhiteSpace(dir))
            Directory.CreateDirectory(dir);
    }

    public void Info(string message) { Write("INFO", message); }
    public void Warn(string message) { Write("WARNING", message); }

    private void Write(string level, string message)
    {
        // Timestamp всегда в invariant формате (чтобы лог был единый на любых локалях)
        string ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
        string line = ts + " - " + level + " - " + message + Environment.NewLine;

        // Лочим запись в файл на случай многопоточности/параллельных вызовов
        lock (_lock)
        {
            File.AppendAllText(_logPath, line, new UTF8Encoding(false));
        }
    }
}

// swaps.csv модели/парсер

internal sealed class SwapSourceRow
{
    // LP-символ (как он приходит из swap_markup.py)
    public string LpSymbol { get; private set; }

    // Источник / LP провайдер (velocity / isprime)
    public string Source { get; private set; }

    // Новые свопы, которые надо применить в TTS
    public decimal NewSwapLong { get; private set; }
    public decimal NewSwapShort { get; private set; }

    // Группа (опционально, для логов/аналитики)
    public string Group { get; private set; }

    public SwapSourceRow(string lpSymbol, string source, decimal newSwapLong, decimal newSwapShort, string group)
    {
        LpSymbol = lpSymbol;
        Source = source;
        NewSwapLong = newSwapLong;
        NewSwapShort = newSwapShort;
        Group = group ?? "";
    }
}

internal static class SwapsCsv
{
    public static List<SwapSourceRow> Load(string path)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException("swaps.csv not found", path);

        // Читаем весь файл (для наших размеров этого достаточно)
        string[] lines = File.ReadAllLines(path);
        if (lines.Length < 2) return new List<SwapSourceRow>();

        // Определяем разделитель: ; или , или \t
        char sep = DetectSeparator(lines[0]);

        // Заголовки приводим к списку
        List<string> headers = SplitCsvLine(lines[0], sep).Select(h => (h ?? "").Trim()).ToList();

        // По факту в нашем swaps.csv обычно такой формат:
        // Symbol,RawSwapLong,RawSwapShort,NewSwapLong,NewSwapShort,Source,Group
        int idxSym = IndexOf(headers, new[] { "symbol", "symbols" });
        int idxNewL = IndexOf(headers, new[] { "newswaplong", "new_swap_long", "swap_long_new" });
        int idxNewS = IndexOf(headers, new[] { "newswapshort", "new_swap_short", "swap_short_new" });
        int idxSource = IndexOf(headers, new[] { "source", "lp_source", "provider" });
        int idxGroup = IndexOf(headers, new[] { "group" });

        if (idxSym < 0 || idxNewL < 0 || idxNewS < 0 || idxSource < 0)
            throw new InvalidOperationException("swaps.csv must contain columns: Symbol, NewSwapLong, NewSwapShort, Source. (Group optional)");

        var res = new List<SwapSourceRow>(lines.Length - 1);

        for (int i = 1; i < lines.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i])) continue;

            List<string> cols = SplitCsvLine(lines[i], sep);

            string lpSym = Get(cols, idxSym);
            if (string.IsNullOrWhiteSpace(lpSym)) continue;

            string source = Get(cols, idxSource);
            if (string.IsNullOrWhiteSpace(source)) source = "";

            string group = (idxGroup >= 0) ? Get(cols, idxGroup) : "";

            decimal newL = ParseDecimalAny(Get(cols, idxNewL));
            decimal newS = ParseDecimalAny(Get(cols, idxNewS));

            res.Add(new SwapSourceRow(lpSym.Trim(), source.Trim(), newL, newS, (group ?? "").Trim()));
        }

        return res;
    }

    static decimal ParseDecimalAny(string v)
    {
        // Парсим decimal независимо от локали:
        // сначала invariant (точка), затем текущая культура (запятая)
        if (string.IsNullOrWhiteSpace(v)) return 0m;
        if (decimal.TryParse(v, NumberStyles.Any, CultureInfo.InvariantCulture, out var d)) return d;
        if (decimal.TryParse(v, NumberStyles.Any, CultureInfo.CurrentCulture, out d)) return d;
        return 0m;
    }

    static int IndexOf(List<string> headers, string[] names)
    {
        // Ищем индекс колонки по нескольким возможным именам
        for (int i = 0; i < headers.Count; i++)
        {
            string h = Normalize(headers[i]);
            for (int j = 0; j < names.Length; j++)
            {
                if (string.Equals(h, Normalize(names[j]), StringComparison.OrdinalIgnoreCase))
                    return i;
            }
        }
        return -1;
    }

    static string Normalize(string s) => (s ?? "").Replace(" ", "").Replace("-", "").Replace("_", "").Trim();

    static string Get(List<string> cols, int idx)
        => (idx >= 0 && idx < cols.Count) ? (cols[idx] ?? "").Trim() : "";

    static char DetectSeparator(string line)
    {
        if (line.IndexOf(';') >= 0 && line.IndexOf(',') < 0) return ';';
        if (line.IndexOf('\t') >= 0) return '\t';
        return ',';
    }

    static List<string> SplitCsvLine(string line, char sep)
    {
        // Простой CSV-сплиттер с поддержкой кавычек.
        // Нам достаточно “простого” парсера для наших файлов.
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

// mapping.csv модели/парсер

internal sealed class MappingRow
{
    // Доп. поля на будущее (сейчас не обязательны)
    public string Mt4 = "";
    public string Mt5 = "";

    // Основные поля: что мы реально используем
    public string Tts = "";
    public string Lp = "";
    public string Source = "";
}

internal static class MappingCsv
{
    public static List<MappingRow> Load(string path)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException("mapping.csv not found", path);

        string[] lines = File.ReadAllLines(path);
        if (lines.Length < 2) return new List<MappingRow>();

        char sep = DetectSeparator(lines[0]);
        List<string> headers = SplitCsvLine(lines[0], sep).Select(h => (h ?? "").Trim()).ToList();

        // В mapping.csv минимально должны быть tts, lp, source
        int idxTts = IndexOf(headers, "tts");
        int idxLp = IndexOf(headers, "lp");
        int idxSource = IndexOf(headers, "source");

        // mt4/mt5 могут отсутствовать — это нормально
        int idxMt4 = IndexOf(headers, "mt4");
        int idxMt5 = IndexOf(headers, "mt5");

        if (idxTts < 0 || idxLp < 0 || idxSource < 0)
            throw new InvalidOperationException("mapping.csv must contain columns 'tts', 'lp', 'source'.");

        var res = new List<MappingRow>(lines.Length - 1);

        for (int i = 1; i < lines.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i])) continue;

            List<string> cols = SplitCsvLine(lines[i], sep);

            var r = new MappingRow
            {
                Mt4 = Get(cols, idxMt4),
                Mt5 = Get(cols, idxMt5),
                Tts = Get(cols, idxTts),
                Lp = Get(cols, idxLp),
                Source = Get(cols, idxSource),
            };

            res.Add(r);
        }

        return res;
    }

    static int IndexOf(List<string> headers, string name)
    {
        // Тут делаем строгий поиск без “Normalize”,
        // потому что mapping.csv у нас стабильно в нижнем регистре.
        for (int i = 0; i < headers.Count; i++)
            if (string.Equals(headers[i], name, StringComparison.OrdinalIgnoreCase))
                return i;
        return -1;
    }

    static string Get(List<string> cols, int idx)
        => (idx >= 0 && idx < cols.Count) ? (cols[idx] ?? "").Trim() : "";

    static char DetectSeparator(string line)
    {
        if (line.IndexOf(';') >= 0 && line.IndexOf(',') < 0) return ';';
        if (line.IndexOf('\t') >= 0) return '\t';
        return ',';
    }

    static List<string> SplitCsvLine(string line, char sep)
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

// Индекс маппинга: (lp + source) -> tts_symbol
internal sealed class MappingIndex
{
    private readonly Dictionary<string, string> _byLpSource;

    private MappingIndex(Dictionary<string, string> byLpSource)
    {
        _byLpSource = byLpSource;
    }

    public static MappingIndex Build(List<MappingRow> rows)
    {
        var byLpSource = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < rows.Count; i++)
        {
            var r = rows[i];

            string lp = (r.Lp ?? "").Trim();
            string tts = (r.Tts ?? "").Trim();
            string srcRaw = (r.Source ?? "").Trim();
            string src = SourceNormalizer.Normalize(srcRaw);

            // Если чего-то не хватает — просто пропускаем строку
            if (string.IsNullOrWhiteSpace(lp) || string.IsNullOrWhiteSpace(tts) || string.IsNullOrWhiteSpace(src))
                continue;

            string key = MakeKey(lp, src);

            // Если вдруг в mapping.csv конфликт (одна связка lp+source ведёт в разные tts) —
            // лучше упасть сразу, чем пушить непредсказуемо.
            if (byLpSource.TryGetValue(key, out var existing))
            {
                if (!string.Equals(existing, tts, StringComparison.OrdinalIgnoreCase))
                    throw new InvalidOperationException(
                        "mapping.csv conflict for lp+source: lp='" + lp + "' source='" + src +
                        "' => '" + existing + "' vs '" + tts + "'"
                    );
            }
            else
            {
                byLpSource[key] = tts;
            }
        }

        return new MappingIndex(byLpSource);
    }

    public string? ResolveTtsSymbolStrict(string lpSymbol, string source, out string error)
    {
        error = "";

        lpSymbol = (lpSymbol ?? "").Trim();
        source = SourceNormalizer.Normalize(source);

        if (string.IsNullOrWhiteSpace(lpSymbol))
        {
            error = "empty LP symbol";
            return null;
        }
        if (string.IsNullOrWhiteSpace(source))
        {
            error = "empty source";
            return null;
        }

        string key = MakeKey(lpSymbol, source);

        if (_byLpSource.TryGetValue(key, out var tts))
            return tts;

        error = "no mapping for lp+source";
        return null;
    }

    private static string MakeKey(string lp, string src) => lp.Trim() + "||" + src.Trim();
}

// Модели для загрузки

internal sealed class SwapUploadRow
{
    public string TtsSymbol;
    public string LpSymbol;

    // Нормализованный source (velocity/isprime)
    public string Source;

    // Как было в исходном swaps.csv (для логов)
    public string RawSource;

    public decimal SwapLong;
    public decimal SwapShort;

    public string Group;

    public SwapUploadRow(string ttsSymbol, string lpSymbol, string source, string rawSource, decimal swapLong, decimal swapShort, string group)
    {
        TtsSymbol = ttsSymbol;
        LpSymbol = lpSymbol;
        Source = source;
        RawSource = rawSource;
        SwapLong = swapLong;
        SwapShort = swapShort;
        Group = group ?? "";
    }
}

internal sealed class UploadResult
{
    public int Total;
    public int Changed;
    public int Skipped;
    public int Failed;
    public List<string> Errors = new List<string>();
}

// Основной класс, который:
// - грузит DLL из libs
// - через reflection находит TickTraderManagerModel
// - коннектится и пушит ModifySymbol / BulkModifySymbol
internal sealed class TickTraderSwapsUploader
{
    private readonly string _libsDir;

    // Иногда в сообщении ошибки встречается “current version is X” — парсим его
    private static readonly Regex ReCurrentVersion =
        new Regex(@"current version is\s+(\d+)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    public TickTraderSwapsUploader(string libsDir)
    {
        _libsDir = libsDir;
        if (!Directory.Exists(_libsDir))
            throw new DirectoryNotFoundException("libs dir not found: " + _libsDir);
    }

    public UploadResult UploadSwaps(
        string server,
        string login,
        string password,
        List<SwapUploadRow> swaps,
        bool dryRun,
        bool onlyIfDifferent,
        FileLogger logger,
        HashSet<string> demoMissingSymbols)
    {
        // Подключаем resolver, чтобы зависимые dll тоже подхватывались из libs
        AppDomain.CurrentDomain.AssemblyResolve += ResolveFromLibs;

        // Загружаем все dll из libs/*.dll
        string[] dlls = Directory.GetFiles(_libsDir, "*.dll", SearchOption.TopDirectoryOnly);
        var loadedAssemblies = new List<Assembly>();

        for (int i = 0; i < dlls.Length; i++)
        {
            var asm = TryLoad(dlls[i]);
            if (asm != null) loadedAssemblies.Add(asm);
        }

        if (loadedAssemblies.Count == 0)
            throw new InvalidOperationException("No assemblies loaded from libs/*.dll. Check libs folder: " + _libsDir);

        // Собираем все типы из загруженных сборок (аккуратно, чтобы не падать на TypeLoadException)
        var allTypes = new List<Type>();
        for (int i = 0; i < loadedAssemblies.Count; i++)
            allTypes.AddRange(SafeGetTypes(loadedAssemblies[i]));

        // Ищем основной тип менеджера
        Type? managerType = allTypes.FirstOrDefault(t =>
            string.Equals(t.FullName, "TickTrader.Manager.Model.TickTraderManagerModel", StringComparison.Ordinal));

        if (managerType == null)
            throw new InvalidOperationException("Could not locate manager type: TickTrader.Manager.Model.TickTraderManagerModel");

        // Создаём instance менеджера
        object? manager = Activator.CreateInstance(managerType);
        if (manager == null)
            throw new InvalidOperationException("Failed to create manager instance.");

        // Коннектимся к серверу TTS
        logger.Info("Connecting to TTS...");
        InvokeConnect(manager, server, login, password);
        logger.Info("Connected.");

        // Тип запроса, через который меняем параметры инструмента
        Type? symbolModifyReqType = allTypes.FirstOrDefault(t =>
            string.Equals(t.FullName, "TickTrader.BusinessObjects.Requests.SymbolModifyRequest", StringComparison.Ordinal));

        if (symbolModifyReqType == null)
            throw new InvalidOperationException("Could not locate request type: TickTrader.BusinessObjects.Requests.SymbolModifyRequest");

        var res = new UploadResult { Total = swaps.Count };

        // Если это demo и в YAML задан список “отсутствующих символов” —
        // мы их сразу пропускаем, чтобы не ловить ошибки
        bool hasDemoSkips = demoMissingSymbols != null && demoMissingSymbols.Count > 0;

        var filtered = new List<SwapUploadRow>(swaps.Count);
        for (int i = 0; i < swaps.Count; i++)
        {
            var row = swaps[i];

            if (hasDemoSkips && demoMissingSymbols.Contains((row.TtsSymbol ?? "").Trim()))
            {
                res.Skipped++;
                logger.Info("SKIP [demo_missing_symbol]: " + row.TtsSymbol + " | lp=" + row.LpSymbol + " | source=" + row.Source + " | group=" + row.Group);
                continue;
            }

            filtered.Add(row);
        }

        if (filtered.Count == 0)
        {
            logger.Info("END PushSwaps | nothing to upload after demo filter.");
            TryInvoke0(manager, new[] { "Disconnect", "Close", "Logout" });
            return res;
        }

        // ConfigVersion в TTS должен увеличиваться последовательно при ModifySymbol,
        // иначе ловим “Configuration version conflict”
        int cfgVersion = ReadConfigVersion(manager);
        logger.Info("Initial ConfigVersion=" + cfgVersion);

        // Если на данном менеджере есть BulkModifySymbol — используем его
        // (он быстрее: одним вызовом пушит пачку запросов)
        MethodInfo? bulkMethod = FindBulkModifySymbol(manager);

        if (bulkMethod != null)
        {
            logger.Info("BulkModifySymbol detected. Using bulk upload.");

            int batchSize = 200;

            // Отправляем чанками по 200 инструментов
            for (int i = 0; i < filtered.Count; i += batchSize)
            {
                var chunk = filtered.Skip(i).Take(batchSize).ToList();

                // В dryRun ничего не отправляем, только пишем что “было бы сделано”
                if (dryRun)
                {
                    for (int j = 0; j < chunk.Count; j++)
                    {
                        var row = chunk[j];
                        res.Skipped++;
                        logger.Info("DRYRUN [" + row.Source + "]: " + row.TtsSymbol + " | lp=" + row.LpSymbol + " | group=" + row.Group +
                                    " | would set Long=" + Fmt(row.SwapLong) + " Short=" + Fmt(row.SwapShort));
                    }
                    continue;
                }

                // Создаём List<SymbolModifyRequest>
                object reqList = CreateTypedRequestList(symbolModifyReqType);

                // Внутри чанка ConfigVersion должен быть последовательный: cfgVersion + 0, +1, +2 ...
                for (int j = 0; j < chunk.Count; j++)
                {
                    var row = chunk[j];

                    object? req = Activator.CreateInstance(symbolModifyReqType);
                    if (req == null)
                        throw new InvalidOperationException("Failed to create SymbolModifyRequest instance.");

                    SetProperty(req, "SymbolName", row.TtsSymbol);
                    SetProperty(req, "SwapSizeLong", row.SwapLong);
                    SetProperty(req, "SwapSizeShort", row.SwapShort);
                    SetPropertyIfExists(req, "SwapEnabled", true);

                    SetProperty(req, "ConfigVersion", cfgVersion + j);

                    AddToTypedList(reqList, req);
                }

                bool ok = false;
                Exception? last = null;

                // На bulk делаем максимум 2 попытки:
                // если конфликт версии — перечитываем ConfigVersion и переставляем версии в чанк
                for (int attempt = 1; attempt <= 2; attempt++)
                {
                    try
                    {
                        InvokeBulkModifySymbol(manager, bulkMethod, reqList);
                        ok = true;
                        break;
                    }
                    catch (Exception ex)
                    {
                        last = ex;
                        string msg = ex.Message ?? "";

                        if (msg.IndexOf("Configuration version conflict", StringComparison.OrdinalIgnoreCase) >= 0)
                        {
                            int fresh = ReadConfigVersion(manager);
                            logger.Warn("WARNING [bulk]: version_conflict attempt=" + attempt + " | reread cv=" + fresh + " | " + msg);

                            cfgVersion = fresh;
                            ResetChunkConfigVersions(reqList, cfgVersion);

                            Thread.Sleep(80);
                            continue;
                        }

                        break;
                    }
                }

                // Если bulk не удалось — падаем на fallback “по одному” для этого чанка
                if (!ok)
                {
                    logger.Warn("WARNING [bulk]: failed. Fallback to single ModifySymbol for this chunk. reason=" + (last != null ? last.Message : "unknown"));
                    UploadChunkSingle(manager, symbolModifyReqType, chunk, ref cfgVersion, logger, res, dryRun);
                    continue;
                }

                // Если bulk прошёл — считаем что все изменения применились
                for (int j = 0; j < chunk.Count; j++)
                {
                    var row = chunk[j];
                    res.Changed++;
                    logger.Info("SUCCESS [" + row.Source + "]: " + row.TtsSymbol + " | lp=" + row.LpSymbol +
                                " | group=" + row.Group + " | NewLong=" + Fmt(row.SwapLong) + " NewShort=" + Fmt(row.SwapShort));
                }

                cfgVersion += chunk.Count;
            }
        }
        else
        {
            // Если BulkModifySymbol нет — работаем по одному символу
            logger.Warn("BulkModifySymbol not found. Using single ModifySymbol.");
            UploadChunkSingle(manager, symbolModifyReqType, filtered, ref cfgVersion, logger, res, dryRun);
        }

        // Закрываем соединение с TTS
        TryInvoke0(manager, new[] { "Disconnect", "Close", "Logout" });
        logger.Info("Disconnected from TTS.");

        return res;
    }

    // BULK helpers

    private static MethodInfo? FindBulkModifySymbol(object manager)
    {
        // Ищем метод BulkModifySymbol(requests)
        var methods = manager.GetType().GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => string.Equals(m.Name, "BulkModifySymbol", StringComparison.OrdinalIgnoreCase) && m.GetParameters().Length == 1)
            .ToList();

        if (methods.Count == 0) return null;
        return methods[0];
    }

    private static void InvokeBulkModifySymbol(object manager, MethodInfo bulkMethod, object requestsList)
    {
        // BulkModifySymbol может возвращать Task — тогда дожидаемся .Result
        try
        {
            var res = bulkMethod.Invoke(manager, new object[] { requestsList });

            if (res != null && res.GetType().Name.StartsWith("Task", StringComparison.OrdinalIgnoreCase))
            {
                var rp = res.GetType().GetProperty("Result");
                if (rp != null) rp.GetValue(res, null);
            }
        }
        catch (TargetInvocationException tie)
        {
            string msg = (tie.InnerException != null) ? tie.InnerException.Message : tie.Message;
            throw new InvalidOperationException("BulkModifySymbol failed: " + msg, tie.InnerException ?? tie);
        }
    }

    private static object CreateTypedRequestList(Type symbolModifyReqType)
    {
        // Создаём List<SymbolModifyRequest> динамически (тип известен только в runtime)
        var listType = typeof(List<>).MakeGenericType(symbolModifyReqType);
        return Activator.CreateInstance(listType)!;
    }

    private static void AddToTypedList(object typedList, object item)
    {
        // typedList.Add(item) через reflection
        var add = typedList.GetType().GetMethod("Add");
        add!.Invoke(typedList, new object[] { item });
    }

    private static void ResetChunkConfigVersions(object reqList, int startCfgVersion)
    {
        // Если ConfigVersion “уехал”, пересчитываем для всего чанка заново
        var idxer = reqList.GetType().GetProperty("Item");
        var countProp = reqList.GetType().GetProperty("Count");
        int count = (int)countProp!.GetValue(reqList, null)!;

        for (int i = 0; i < count; i++)
        {
            object req = idxer!.GetValue(reqList, new object[] { i })!;
            SetProperty(req, "ConfigVersion", startCfgVersion + i);
        }
    }

    // Single upload

    private void UploadChunkSingle(
        object manager,
        Type symbolModifyReqType,
        List<SwapUploadRow> chunk,
        ref int cfgVersion,
        FileLogger logger,
        UploadResult res,
        bool dryRun)
    {
        // Пушим по одному инструменту.
        // Здесь делаем больше попыток (до 10), потому что конфликты версии могут быть частыми.
        for (int i = 0; i < chunk.Count; i++)
        {
            var row = chunk[i];

            try
            {
                if (dryRun)
                {
                    res.Skipped++;
                    logger.Info("DRYRUN [" + row.Source + "]: " + row.TtsSymbol + " | lp=" + row.LpSymbol + " | group=" + row.Group +
                                " | would set Long=" + Fmt(row.SwapLong) + " Short=" + Fmt(row.SwapShort));
                    continue;
                }

                object? req = Activator.CreateInstance(symbolModifyReqType);
                if (req == null)
                    throw new InvalidOperationException("Failed to create SymbolModifyRequest instance.");

                SetProperty(req, "SymbolName", row.TtsSymbol);
                SetProperty(req, "SwapSizeLong", row.SwapLong);
                SetProperty(req, "SwapSizeShort", row.SwapShort);
                SetPropertyIfExists(req, "SwapEnabled", true);

                bool ok = false;
                Exception? last = null;

                for (int attempt = 1; attempt <= 10; attempt++)
                {
                    try
                    {
                        // ВАЖНО: ConfigVersion должен быть текущим ожидаемым значением
                        SetProperty(req, "ConfigVersion", cfgVersion);
                        InvokeModifySymbol(manager, req);

                        ok = true;

                        // Если получилось не с первой попытки — логируем что был retry
                        if (attempt > 1)
                            logger.Info("INFO [" + row.Source + "]: " + row.TtsSymbol + " | retry_ok attempt=" + attempt + " cv=" + cfgVersion);

                        cfgVersion++;
                        break;
                    }
                    catch (Exception ex)
                    {
                        last = ex;
                        string msg = ex.Message ?? "";

                        // Если это не конфликт версии — нет смысла ретраить
                        if (msg.IndexOf("Configuration version conflict", StringComparison.OrdinalIgnoreCase) < 0)
                            break;

                        // Иногда в тексте ошибки уже есть “current version is X”
                        // Тогда просто берём X и продолжаем
                        if (TryParseCurrentVersion(msg, out var parsedCurrent))
                        {
                            logger.Warn("WARNING [" + row.Source + "]: " + row.TtsSymbol +
                                        " | version_conflict attempt=" + attempt + " | set cv=" + parsedCurrent + " | " + msg);
                            cfgVersion = parsedCurrent;
                            Thread.Sleep(30);
                            continue;
                        }

                        // Если распарсить не смогли — перечитываем ConfigVersion с менеджера
                        int fresh = ReadConfigVersion(manager);
                        logger.Warn("WARNING [" + row.Source + "]: " + row.TtsSymbol +
                                    " | version_conflict attempt=" + attempt + " | reread cv=" + fresh + " | " + msg);
                        cfgVersion = fresh;
                        Thread.Sleep(60);
                    }
                }

                if (!ok)
                    throw last ?? new InvalidOperationException("ModifySymbol failed (unknown).");

                res.Changed++;
                logger.Info("SUCCESS [" + row.Source + "]: " + row.TtsSymbol + " | lp=" + row.LpSymbol +
                            " | group=" + row.Group + " | NewLong=" + Fmt(row.SwapLong) + " NewShort=" + Fmt(row.SwapShort));
            }
            catch (Exception ex)
            {
                // Ошибку по одному символу не валим весь процесс — просто считаем как failed
                res.Failed++;
                string msg = "ERROR [" + row.Source + "]: " + row.TtsSymbol + " | lp=" + row.LpSymbol +
                             " | group=" + row.Group + " | reason=" + ex.Message;
                res.Errors.Add(msg);
                logger.Warn(msg);
            }
        }
    }

    // Reflection helpers

    private bool TryParseCurrentVersion(string message, out int currentVersion)
    {
        // Пытаемся вытащить “current version is 123” из текста ошибки
        currentVersion = 0;
        if (string.IsNullOrWhiteSpace(message)) return false;

        var m = ReCurrentVersion.Match(message);
        if (!m.Success) return false;

        if (!int.TryParse(m.Groups[1].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v))
            return false;

        currentVersion = v;
        return true;
    }

    private Assembly? ResolveFromLibs(object? sender, ResolveEventArgs args)
    {
        // Когда CLR не может найти зависимую сборку —
        // пробуем найти её прямо в libsDir по имени.
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

    private static Assembly? TryLoad(string dllPath)
    {
        // Не хотим падать, если какая-то dll не грузится (например, лишняя/битая)
        try { return Assembly.LoadFrom(dllPath); }
        catch { return null; }
    }

    private static List<Type> SafeGetTypes(Assembly asm)
    {
        // Достаём типы из сборки так, чтобы не падать на ReflectionTypeLoadException
        try { return asm.GetTypes().Where(t => t != null).ToList(); }
        catch (ReflectionTypeLoadException rtle)
        {
            return rtle.Types.Where(t => t != null).ToList()!;
        }
        catch
        {
            return new List<Type>();
        }
    }

    private static void InvokeConnect(object manager, string server, string login, string password)
    {
        // В разных версиях библиотек сигнатура Connect может отличаться:
        // - Connect(string, long, string)
        // - Connect(string, string, string)
        // Поэтому ищем метод по имени и пробуем обе сигнатуры.
        var methods = manager.GetType().GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => string.Equals(m.Name, "Connect", StringComparison.OrdinalIgnoreCase))
            .ToList();

        foreach (var m in methods)
        {
            var ps = m.GetParameters();

            try
            {
                // Connect(string server, long login, string password)
                if (ps.Length == 3 &&
                    ps[0].ParameterType == typeof(string) &&
                    (ps[1].ParameterType == typeof(long) || ps[1].ParameterType == typeof(long)) &&
                    ps[2].ParameterType == typeof(string))
                {
                    if (!long.TryParse(login, out var loginLong))
                        throw new InvalidOperationException("Login '" + login + "' is not a valid Int64");

                    m.Invoke(manager, new object[] { server, loginLong, password });
                    return;
                }

                // Connect(string server, string login, string password)
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
                string msg = tie.InnerException?.Message ?? tie.Message;
                throw new InvalidOperationException("Connect failed: " + msg, tie.InnerException ?? tie);
            }
        }

        throw new InvalidOperationException("Could not invoke Connect().");
    }

    private static void InvokeModifySymbol(object manager, object request)
    {
        // Ищем ModifySymbol(request)
        var m = manager.GetType().GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault(x => string.Equals(x.Name, "ModifySymbol", StringComparison.OrdinalIgnoreCase) &&
                                 x.GetParameters().Length == 1);

        if (m == null)
            throw new InvalidOperationException("ModifySymbol(...) method not found on manager.");

        try
        {
            var res = m.Invoke(manager, new object[] { request });

            // Если это Task — дожидаемся выполнения
            if (res != null && res.GetType().Name.StartsWith("Task", StringComparison.OrdinalIgnoreCase))
            {
                var rp = res.GetType().GetProperty("Result");
                if (rp != null) rp.GetValue(res, null);
            }
        }
        catch (TargetInvocationException tie)
        {
            string msg = tie.InnerException?.Message ?? tie.Message;
            throw new InvalidOperationException("ModifySymbol failed: " + msg, tie.InnerException ?? tie);
        }
    }

    private static int ReadConfigVersion(object manager)
    {
        // Получаем manager.ConfigVersion через reflection
        var p = manager.GetType().GetProperty("ConfigVersion", BindingFlags.Public | BindingFlags.Instance);
        if (p == null)
            throw new InvalidOperationException("Manager.ConfigVersion property not found.");

        var v = p.GetValue(manager, null);
        if (v is int i) return i;

        if (v != null && int.TryParse(v.ToString(), out var parsed))
            return parsed;

        throw new InvalidOperationException("Cannot read ConfigVersion from manager.");
    }

    private static void SetProperty(object obj, string propName, object value)
    {
        // Устанавливаем свойство через reflection,
        // при этом аккуратно приводим типы (decimal->float/double и т.п.)
        var p = obj.GetType().GetProperty(propName, BindingFlags.Public | BindingFlags.Instance);
        if (p == null)
            throw new InvalidOperationException("Property not found: " + obj.GetType().FullName + "." + propName);

        if (value == null)
        {
            p.SetValue(obj, null, null);
            return;
        }

        Type targetType = Nullable.GetUnderlyingType(p.PropertyType) ?? p.PropertyType;

        if (targetType == typeof(float))
        {
            float f;
            if (value is decimal dec) f = (float)dec;
            else if (value is double db) f = (float)db;
            else if (value is float fl) f = fl;
            else f = Convert.ToSingle(value, CultureInfo.InvariantCulture);

            p.SetValue(obj, f, null);
            return;
        }

        if (targetType == typeof(double))
        {
            double d;
            if (value is decimal dec) d = (double)dec;
            else if (value is float fl) d = fl;
            else if (value is double db) d = db;
            else d = Convert.ToDouble(value, CultureInfo.InvariantCulture);

            p.SetValue(obj, d, null);
            return;
        }

        if (targetType == typeof(decimal))
        {
            decimal d = (value is decimal dec) ? dec : Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            p.SetValue(obj, d, null);
            return;
        }

        if (targetType == typeof(int))
        {
            int i = (value is int ii) ? ii : Convert.ToInt32(value, CultureInfo.InvariantCulture);
            p.SetValue(obj, i, null);
            return;
        }

        if (targetType == typeof(long))
        {
            long l = (value is long ll) ? ll : Convert.ToInt64(value, CultureInfo.InvariantCulture);
            p.SetValue(obj, l, null);
            return;
        }

        if (targetType == typeof(bool))
        {
            bool b = (value is bool bb) ? bb : Convert.ToBoolean(value, CultureInfo.InvariantCulture);
            p.SetValue(obj, b, null);
            return;
        }

        if (targetType == typeof(string))
        {
            p.SetValue(obj, value.ToString(), null);
            return;
        }

        p.SetValue(obj, value, null);
    }

    private static void SetPropertyIfExists(object obj, string propName, object value)
    {
        // Иногда в разных версиях DLL свойство может отсутствовать (например, SwapEnabled),
        // поэтому ставим “если есть” и не падаем.
        var p = obj.GetType().GetProperty(propName, BindingFlags.Public | BindingFlags.Instance);
        if (p == null) return;
        try { SetProperty(obj, propName, value); } catch { }
    }

    private static void TryInvoke0(object obj, string[] methodNames)
    {
        // Пытаемся вызвать один из методов без параметров:
        // Disconnect / Close / Logout
        for (int i = 0; i < methodNames.Length; i++)
        {
            string name = methodNames[i];

            var m = obj.GetType().GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase) &&
                                     x.GetParameters().Length == 0);

            if (m == null) continue;
            try { m.Invoke(obj, null); } catch { }
            return;
        }
    }

    static string Fmt(decimal v) => v.ToString("0.#####", CultureInfo.InvariantCulture);
}

// Минимальный YAML reader для config.yaml.
// Нам нужно только:
// - tts_configs (берём первый server/login/password)
// - tts_uploader.demo_missing_symbols.<server> -> list
internal sealed class SimpleYaml
{
    public TtsConfig FirstTts = new TtsConfig();

    // demo_missing_symbols[server] -> list
    private readonly Dictionary<string, List<string>> _demoMissingByServer =
        new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

    public static SimpleYaml Load(string path)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException("config.yaml not found", path);

        string[] lines = File.ReadAllLines(path);
        var cfg = new SimpleYaml();

        bool inTtsConfigs = false;
        bool inUploader = false;
        bool inDemoMissing = false;
        string? currentDemoServer = null;

        for (int i = 0; i < lines.Length; i++)
        {
            string raw = lines[i];
            if (raw == null) continue;

            // Пропускаем пустые строки и комментарии
            string lineTrim = raw.Trim();
            if (string.IsNullOrWhiteSpace(lineTrim) || lineTrim.StartsWith("#")) continue;

            // Нам важно сохранить отступы, чтобы понимать в каком блоке мы находимся
            int indent = CountLeadingSpaces(raw);

            // Вход в блок tts_configs:
            if (indent == 0 && lineTrim.StartsWith("tts_configs:", StringComparison.OrdinalIgnoreCase))
            {
                inTtsConfigs = true;
                inUploader = false;
                inDemoMissing = false;
                currentDemoServer = null;
                continue;
            }

            // Вход в блок tts_uploader:
            if (indent == 0 && lineTrim.StartsWith("tts_uploader:", StringComparison.OrdinalIgnoreCase))
            {
                inUploader = true;
                inTtsConfigs = false;
                inDemoMissing = false;
                currentDemoServer = null;
                continue;
            }

            // Если мы внутри tts_uploader
            if (inUploader)
            {
                // Ищем подпункт demo_missing_symbols:
                if (indent == 2 && lineTrim.StartsWith("demo_missing_symbols:", StringComparison.OrdinalIgnoreCase))
                {
                    inDemoMissing = true;
                    currentDemoServer = null;
                    continue;
                }

                // Если мы внутри demo_missing_symbols
                if (inDemoMissing)
                {
                    // Ключ сервера выглядит как:
                    // xxxxxx.fxopen.com:
                    if (indent == 4 && lineTrim.EndsWith(":") && !lineTrim.StartsWith("-"))
                    {
                        currentDemoServer = lineTrim.Substring(0, lineTrim.Length - 1).Trim();
                        if (!cfg._demoMissingByServer.ContainsKey(currentDemoServer))
                            cfg._demoMissingByServer[currentDemoServer] = new List<string>();
                        continue;
                    }

                    // Элементы списка:
                    // - EURCZK
                    if (indent >= 6 && lineTrim.StartsWith("-", StringComparison.Ordinal))
                    {
                        if (!string.IsNullOrWhiteSpace(currentDemoServer))
                        {
                            string sym = lineTrim.Substring(1).Trim();
                            if (!string.IsNullOrWhiteSpace(sym))
                                cfg._demoMissingByServer[currentDemoServer].Add(sym);
                        }
                        continue;
                    }

                    // Если выходим из блока по отступам
                    if (indent < 2)
                    {
                        inDemoMissing = false;
                        currentDemoServer = null;
                    }
                }

                continue;
            }

            // Если мы внутри tts_configs (берём только первый конфиг)
            if (inTtsConfigs)
            {
                // В YAML список часто выглядит так:
                // - server: ...
                // - login: ...
                if (lineTrim.StartsWith("-", StringComparison.Ordinal))
                {
                    string rest = lineTrim.Substring(1).Trim();
                    TryReadTtsKeyValue(cfg, rest);
                    continue;
                }

                // Или так:
                // server: ...
                TryReadTtsKeyValue(cfg, lineTrim);
                continue;
            }
        }

        if (string.IsNullOrWhiteSpace(cfg.FirstTts.Server))
            throw new InvalidOperationException("config.yaml: tts_configs[0].server not found");

        return cfg;
    }

    public TtsConfig ReadTtsConfigFirst() => FirstTts;

    public HashSet<string> GetDemoMissingSymbolsForServer(string server)
    {
        // Возвращаем список “missing symbols” как HashSet для быстрого поиска Contains()
        server = (server ?? "").Trim();
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (string.IsNullOrWhiteSpace(server))
            return set;

        if (_demoMissingByServer.TryGetValue(server, out var list))
        {
            for (int i = 0; i < list.Count; i++)
            {
                var s = (list[i] ?? "").Trim();
                if (!string.IsNullOrWhiteSpace(s))
                    set.Add(s);
            }
        }

        return set;
    }

    private static void TryReadTtsKeyValue(SimpleYaml cfg, string line)
    {
        // Парсим строки вида "key: value"
        int idx = line.IndexOf(':');
        if (idx <= 0) return;

        string k = line.Substring(0, idx).Trim();
        string v = Unquote(line.Substring(idx + 1).Trim());

        if (string.Equals(k, "server", StringComparison.OrdinalIgnoreCase)) cfg.FirstTts.Server = v;
        if (string.Equals(k, "login", StringComparison.OrdinalIgnoreCase)) cfg.FirstTts.Login = v;
        if (string.Equals(k, "password", StringComparison.OrdinalIgnoreCase)) cfg.FirstTts.Password = v;
    }

    private static int CountLeadingSpaces(string s)
    {
        int n = 0;
        while (n < s.Length && s[n] == ' ') n++;
        return n;
    }

    private static string Unquote(string v)
    {
        // Убираем кавычки, если они есть
        v = v.Trim();
        if (v.StartsWith("\"") && v.EndsWith("\"") && v.Length >= 2)
            return v.Substring(1, v.Length - 2);
        if (v.StartsWith("'") && v.EndsWith("'") && v.Length >= 2)
            return v.Substring(1, v.Length - 2);
        return v;
    }
}

internal sealed class TtsConfig
{
    public string Server = "";
    public string Login = "";
    public string Password = "";
}

// Парсер аргументов --key value
// (минимальный, но удобный для наших CLI тулов)
internal static class Args
{
    public static Dictionary<string, string> Parse(string[] args)
    {
        var d = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < args.Length; i++)
        {
            var a = args[i];
            if (!a.StartsWith("--")) continue;

            // если после --key стоит значение (и оно не начинается с --), значит это VALUE
            if (i + 1 < args.Length && !args[i + 1].StartsWith("--"))
            {
                d[a] = args[i + 1];
                i++;
            }
            else
            {
                // иначе считаем это флагом (true)
                d[a] = "true";
            }
        }

        return d;
    }
}
