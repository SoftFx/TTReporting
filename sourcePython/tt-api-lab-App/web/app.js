const resultList = document.querySelector("#resultList");
const runLog = document.querySelector("#runLog");
const runSummary = document.querySelector("#runSummary");
const spinner = document.querySelector("#spinner");
const consoleResponse = document.querySelector("#consoleResponse");
const buttons = [...document.querySelectorAll("[data-check]")];
const cards = [...document.querySelectorAll("[data-card]")];

const fields = {
  restPort: document.querySelector("#restPort"),
  symbol: document.querySelector("#symbol"),
  timeout: document.querySelector("#timeout"),
  quoteDownloadApi: document.querySelector("#quoteDownloadApi"),
  quoteHistoryKind: document.querySelector("#quoteHistoryKind"),
  periodicity: document.querySelector("#periodicity"),
  priceType: document.querySelector("#priceType"),
  barsFromDate: document.querySelector("#barsFromDate"),
  barsFromTime: document.querySelector("#barsFromTime"),
  barsToDate: document.querySelector("#barsToDate"),
  barsToTime: document.querySelector("#barsToTime"),
  fixTradePort: document.querySelector("#fixTradePort"),
  fixFeedPort: document.querySelector("#fixFeedPort"),
  fixTls: document.querySelector("#fixTls"),
  fixResetSeqNum: document.querySelector("#fixResetSeqNum"),
  wsFeedPort: document.querySelector("#wsFeedPort"),
  wsTradePort: document.querySelector("#wsTradePort"),
  orderSide: document.querySelector("#orderSide"),
  orderType: document.querySelector("#orderType"),
  orderAmount: document.querySelector("#orderAmount"),
  orderLimitPrice: document.querySelector("#orderLimitPrice"),
  testRequest: document.querySelector("#testRequest"),
  enableTrading: document.querySelector("#enableTrading"),
};

const statusNodes = {
  hmac: document.querySelector("#hmacStatus"),
  fix: document.querySelector("#fixStatus"),
  order: document.querySelector("#orderStatus"),
};

const profileBtns = {
  demo: document.querySelector("#profileDemo"),
  live: document.querySelector("#profileLive"),
  hint: document.querySelector("#profileHint"),
};

profileBtns.demo.addEventListener("click", () => switchProfile("demo"));
profileBtns.live.addEventListener("click", () => switchProfile("live"));

async function switchProfile(name) {
  try {
    const response = await fetch("api/switch-profile", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ profile: name }),
    });
    const data = await response.json();
    if (data.ok) {
      applyStatus(data.status, { preserveInputs: false });
      appendLog(`профиль: ${name}`);
    } else {
      appendLog(`ошибка профиля: ${data.error}`);
    }
  } catch (error) {
    appendLog(`ошибка профиля: ${error.message}`);
  }
}

const consoleFields = {
  api: document.querySelector("#consoleApi"),
  channel: document.querySelector("#consoleChannel"),
  channelField: document.querySelector("#consoleChannelField"),
  auth: document.querySelector("#consoleAuth"),
  authField: document.querySelector("#consoleAuthField"),
  request: document.querySelector("#consoleRequest"),
  send: document.querySelector("#sendConsoleRequest"),
  clear: document.querySelector("#clearConsoleResponse"),
  toggle: document.querySelector("#toggleConsole"),
  body: document.querySelector("#consoleBody"),
};

const labels = {
  public: "Публичный REST",
  "rest-auth": "REST аккаунт",
  "rest-quotes": "REST котировки",
  "rest-trade": "REST торговля",
  bars: "Скачать котировки",
  "fix-trade": "FIX торговля",
  "fix-feed": "FIX котировки",
  "fix-feed-open": "FIX котировки",
  "fix-feed-close": "Закрыть FIX котировки",
  "ws-feed": "WS котировки",
  "ws-feed-open": "WS котировки",
  "ws-feed-close": "Закрыть WS котировки",
  "ws-trade": "WS торговля",
  "order-rest": "Ордер REST",
  "order-fix": "Ордер FIX",
  "order-ws": "Ордер WS",
};

let didLoadStatus = false;
const streamOffsets = {};
const streamTimers = {};

const DEFAULT_SETTINGS = {
  restPort: "443",
  symbol: "EURUSD",
  timeout: "15",
  quoteDownloadApi: "REST",
  quoteHistoryKind: "Ticks",
  periodicity: "M1",
  priceType: "Bid",
  barsFromDate: "",
  barsFromTime: "",
  barsToDate: "",
  barsToTime: "",
  fixTradePort: "5002",
  fixFeedPort: "5001",
  fixTls: false,
  fixResetSeqNum: false,
  testRequest: true,
  wsFeedPort: "443",
  wsTradePort: "443",
  orderSide: "Buy",
  orderType: "Limit",
  orderAmount: "",
  orderLimitPrice: "",
  enableTrading: false,
};

document.querySelector("#refreshStatus").addEventListener("click", resetFormToDefaults);
document.querySelector("#clearLog").addEventListener("click", () => {
  runLog.textContent = "";
});
fields.orderType.addEventListener("change", updateOrderTypeVisibility);
fields.quoteHistoryKind.addEventListener("change", updateHistoryTypeVisibility);
consoleFields.api.addEventListener("change", updateConsoleMode);
consoleFields.send.addEventListener("click", runConsoleRequest);
consoleFields.clear.addEventListener("click", () => {
  consoleResponse.textContent = "";
});
consoleFields.toggle.addEventListener("click", toggleConsole);
Object.values(fields).forEach((field) => {
  field.addEventListener("input", refreshReadinessFromInputs);
  field.addEventListener("change", refreshReadinessFromInputs);
});

buttons.forEach((button) => {
  button.addEventListener("click", () => runCheck(button.dataset.check));
});

refreshStatus();
updateOrderTypeVisibility();
updateHistoryTypeVisibility();
updateConsoleMode();

async function refreshStatus(options = {}) {
  try {
    const response = await fetch("api/status", { cache: "no-store" });
    const status = await response.json();
    applyStatus(status, { preserveInputs: Boolean(options.preserveInputs || didLoadStatus) });
    didLoadStatus = true;
    if (options.log) appendLog("состояние обновлено");
  } catch (error) {
    appendLog(`ошибка состояния: ${error.message}`);
  }
}

function resetFormToDefaults() {
  const boolFields = new Set(["fixTls", "fixResetSeqNum", "testRequest", "enableTrading"]);
  for (const [key, value] of Object.entries(DEFAULT_SETTINGS)) {
    const field = fields[key];
    if (!field) continue;
    if (boolFields.has(key)) {
      field.checked = Boolean(value);
    } else {
      field.value = value;
    }
  }
  updateOrderTypeVisibility();
  refreshReadinessFromInputs();
  appendLog("настройки сброшены");
}

function setDateTimeFields(prefix, value) {
  const dateField = fields[`${prefix}Date`];
  const timeField = fields[`${prefix}Time`];
  if (!dateField || !timeField) return;
  const match = String(value || "").trim().match(/^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})(?::(\d{2}))?/);
  dateField.value = match?.[1] || "";
  timeField.value = match ? `${match[2]}:${match[3] || "00"}` : "";
}

function buildUtcDateTime(dateValue, timeValue) {
  if (!dateValue) return "";
  const parts = String(timeValue || "00:00:00").split(":");
  const hours = (parts[0] || "00").padStart(2, "0");
  const minutes = (parts[1] || "00").padStart(2, "0");
  const seconds = (parts[2] || "00").padStart(2, "0");
  return `${dateValue}T${hours}:${minutes}:${seconds}Z`;
}

function applyStatus(status, options = {}) {
  if (!options.preserveInputs) {
    fields.restPort.value = status.restPort || "443";
    fields.symbol.value = status.symbol || "EURUSD";
    fields.quoteDownloadApi.value = status.quoteDownloadApi || "REST";
    fields.quoteHistoryKind.value = status.quoteHistoryKind || "Ticks";
    fields.periodicity.value = status.periodicity || "M1";
    fields.priceType.value = status.priceType || "Bid";
    setDateTimeFields("barsFrom", status.barsFromUtc || "");
    setDateTimeFields("barsTo", status.barsToUtc || "");
    fields.fixTradePort.value = status.fixTradePort || "5002";
    fields.fixFeedPort.value = status.fixFeedPort || "5001";
    fields.fixTls.checked = status.fix?.tls ?? false;
    fields.fixResetSeqNum.checked = status.fix?.resetSeqNum ?? false;
    fields.wsFeedPort.value = status.wsFeedPort || "443";
    fields.wsTradePort.value = status.wsTradePort || "443";
    fields.orderSide.value = status.order?.side || "Buy";
    fields.orderType.value = status.order?.type || "Limit";
    fields.orderAmount.value = status.order?.amount || "";
    fields.orderLimitPrice.value = status.order?.limitPrice || "";
  }

  const profile = (status.profile || "demo").toLowerCase();
  profileBtns.demo.classList.toggle("active", profile === "demo");
  profileBtns.live.classList.toggle("active", profile === "live");
  const label = profile === "live" ? "Live" : "Demo";
  profileBtns.hint.textContent = `Активен: ${label}`;

  document.querySelector("#connectionLine").textContent =
    `${status.restBaseUrl || "REST"}:${status.restPort || "443"} | FIX ${status.fixHost}:${status.fixTradePort}/${status.fixFeedPort} | WS ${status.wsBaseUrl || "wss://..."}`;

  setPill(statusNodes.hmac, status.hmac?.ready ? "ready" : "fail", status.hmac?.ready ? "REST ключи есть" : `REST: ${missingText(status.hmac?.missing)}`);
  setPill(statusNodes.fix, status.fix?.ready ? "ready" : "fail", status.fix?.ready ? "FIX логин есть" : `FIX: ${missingText(status.fix?.missing)}`);

  const orderState = status.order?.ready ? "ready" : status.order?.armed ? "warn" : "fail";
  const orderText = status.order?.ready ? "Ордер готов" : status.order?.armed ? `Ордер: ${missingText(status.order?.missing)}` : "Ордер закрыт";
  setPill(statusNodes.order, orderState, orderText);

  setCardState("public", "idle", "Готово");
  setCardState("rest-auth", status.hmac?.ready ? "idle" : "locked", status.hmac?.ready ? "Готово" : "Нужны ключи");
  setCardState("rest-quotes", status.hmac?.ready ? "idle" : "locked", status.hmac?.ready ? "Готово" : "Нужны ключи");
  setCardState("rest-trade", status.hmac?.ready ? "idle" : "locked", status.hmac?.ready ? "Готово" : "Нужны ключи");
  setCardState("bars", "idle", "Готово");
  setCardState("fix-trade", status.fix?.ready ? "idle" : "locked", status.fix?.ready ? "Готово" : "Нужен логин");
  setCardState("fix-feed", status.fix?.ready ? "idle" : "locked", status.fix?.ready ? "Готово" : "Нужен логин");
  setCardState("ws-feed", status.hmac?.ready ? "idle" : "locked", status.hmac?.ready ? "Готово" : "Нужны ключи");
  setCardState("ws-trade", status.hmac?.ready ? "idle" : "locked", status.hmac?.ready ? "Готово" : "Нужны ключи");
  ["order-rest", "order-fix", "order-ws"].forEach((name) => {
    setCardState(name, status.order?.ready ? "idle" : "locked", status.order?.ready ? "Готово" : "Закрыто");
  });
  refreshReadinessFromInputs();
  updateOrderTypeVisibility();
  updateHistoryTypeVisibility();
}

function setPill(node, state, text) {
  node.classList.remove("ready", "warn", "fail");
  node.classList.add(state);
  node.textContent = text;
}

function missingText(missing = []) {
  if (!missing.length) return "готово";
  return `нет ${missing.length}`;
}

function payloadFor(check) {
  const fixFeedPort = normalizedFixFeedPort(check);
  return {
    check,
    restPort: fields.restPort.value.trim(),
    symbol: fields.symbol.value.trim(),
    timeout: Number(fields.timeout.value || 15),
    quoteDownloadApi: fields.quoteDownloadApi.value,
    quoteHistoryKind: fields.quoteHistoryKind.value,
    periodicity: fields.periodicity.value,
    priceType: fields.priceType.value,
    barsFromUtc: buildUtcDateTime(fields.barsFromDate.value, fields.barsFromTime.value),
    barsToUtc: buildUtcDateTime(fields.barsToDate.value, fields.barsToTime.value),
    fixTradePort: fields.fixTradePort.value.trim(),
    fixFeedPort,
    fixTls: fields.fixTls.checked,
    fixResetSeqNum: fields.fixResetSeqNum.checked,
    wsFeedPort: fields.wsFeedPort.value.trim(),
    wsTradePort: fields.wsTradePort.value.trim(),
    orderSide: fields.orderSide.value,
    orderType: fields.orderType.value,
    orderAmount: fields.orderAmount.value.trim(),
    orderLimitPrice: fields.orderLimitPrice.value.trim(),
    noBarsFile: false,
    testRequest: fields.testRequest.checked,
    enableTrading: fields.enableTrading.checked,
  };
}

function normalizedFixFeedPort(check) {
  let value = fields.fixFeedPort.value.trim();
  if (check === "fix-feed" || check === "fix-feed-open" || check === "fix-feed-close") {
    if (!value) {
      value = fields.fixTls.checked ? "5003" : "5001";
      fields.fixFeedPort.value = value;
    }
  }
  return value;
}

async function runCheck(check) {
  if (isOrderCheck(check) && !fields.enableTrading.checked) {
    const result = {
      name: "Тест ордера",
      ok: false,
      detail: "Включи переключатель «Разрешить тест ордера».",
    };
    setCardState(check, "fail", "Закрыто");
    renderResults([result]);
    runSummary.textContent = "Тест ордера закрыт";
    appendLog(`[FAIL] ${result.name}: ${result.detail}`);
    return;
  }
  if (isOrderCheck(check) && !confirmOrderRun()) return;

  setBusy(true);
  markRunning(check);
  resultList.innerHTML = "";
  runSummary.textContent = `Проверяю: ${labels[check] || check}`;
  appendLog(`запуск: ${labels[check] || check}`);

  try {
    const response = await fetch("api/check", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payloadFor(check)),
    });
    const data = await response.json();
    renderResults(data.results || []);
    updateCardsAfterRun(check, data.results || [], Boolean(data.ok));
    if (data.status) applyStatus(data.status, { preserveInputs: true });
    updateCardsAfterRun(check, data.results || [], Boolean(data.ok));
    runSummary.textContent = `${data.ok ? "Работает" : "Есть ошибка"}: ${labels[check] || check} (${data.elapsedMs || 0} ms)`;
    appendLog(formatRunLog(check, data));
    if (data.ok && isStreamOpenCheck(check)) startStreamPolling(cardForCheck(check));
    if (data.ok && isStreamCloseCheck(check)) stopStreamPolling(cardForCheck(check));
  } catch (error) {
    setCardState(cardForCheck(check), "fail", "Ошибка");
    runSummary.textContent = "Запрос не прошел";
    renderResults([{ name: "GUI", ok: false, detail: error.message }]);
    appendLog(`ошибка запуска: ${error.message}`);
  } finally {
    setBusy(false);
  }
}

async function runConsoleRequest() {
  const api = consoleFields.api.value;
  const channel = consoleFields.channel.value;
  const pseudoCheck = api === "fix" && channel === "feed" ? "fix-feed" : "console";
  const payload = {
    ...payloadFor(pseudoCheck),
    consoleApi: api,
    consoleChannel: channel,
    consoleAuth: consoleFields.auth.checked,
    consoleRequest: consoleFields.request.value,
  };
  consoleFields.send.disabled = true;
  consoleResponse.textContent = "Отправляю...";
  appendLog(`консоль: ${api.toUpperCase()} ${api === "rest" ? "" : channel}`);
  try {
    const response = await fetch("api/console", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    consoleResponse.textContent = formatConsoleResponse(data);
    if (data.status) applyStatus(data.status, { preserveInputs: true });
    appendLog(formatConsoleLog(api, data));
  } catch (error) {
    consoleResponse.textContent = `ERROR\n${error.message}`;
    appendLog(`консоль ошибка: ${error.message}`);
  } finally {
    consoleFields.send.disabled = false;
  }
}

function formatConsoleResponse(data) {
  const lines = [`${data.ok ? "PASS" : "FAIL"} | ${data.elapsedMs || 0} ms`];
  if (data.commands?.length) {
    lines.push("", "SENT:");
    for (const command of data.commands) lines.push(command);
  }
  if (data.detail) lines.push("", "RESPONSE:", data.detail);
  if (!data.detail && data.results?.length) {
    lines.push("", "RESPONSE:");
    for (const item of data.results) lines.push(`[${item.ok ? "PASS" : "FAIL"}] ${item.name}: ${item.detail}`);
  }
  return lines.join("\n");
}

function formatConsoleLog(api, data) {
  const lines = [`консоль готово: ${api.toUpperCase()} | ${data.ok ? "PASS" : "FAIL"} | ${data.elapsedMs || 0} ms`];
  if (data.commands?.length) {
    lines.push("  SENT:");
    for (const command of data.commands) lines.push(`    ${command}`);
  }
  if (data.detail) lines.push(`  RESPONSE: ${data.detail}`);
  return lines.join("\n");
}

function updateConsoleMode() {
  const api = consoleFields.api.value;
  consoleFields.channelField.classList.toggle("hidden", api === "rest");
  consoleFields.authField.classList.toggle("hidden", api !== "rest");
  if (consoleFields.request.dataset.touched !== "true") {
    consoleFields.request.value = consoleExample(api);
  }
}

function toggleConsole() {
  const hidden = consoleFields.body.classList.toggle("hidden");
  consoleFields.toggle.textContent = hidden ? "Показать" : "Скрыть";
  consoleFields.toggle.setAttribute("aria-expanded", String(!hidden));
}

consoleFields.request.addEventListener("input", () => {
  consoleFields.request.dataset.touched = "true";
});

function consoleExample(api) {
  if (api === "fix") {
    return "35=1|112=console-test";
  }
  if (api === "ws") {
    return '{\"Request\":\"Symbols\",\"Params\":{\"Symbol\":\"EURUSD\"}}';
  }
  return "GET /api/v1/public/tick/EURUSD";
}

function confirmOrderRun() {
  const type = fields.orderType.value;
  const symbol = fields.symbol.value.trim();
  const side = fields.orderSide.value;
  const amount = fields.orderAmount.value.trim();
  const price = fields.orderLimitPrice.value.trim();
  const detail = type === "Market" ? `${side} Market ${amount} ${symbol}` : `${side} Limit ${amount} ${symbol} @ ${price}`;
  return window.confirm(`Запустить тест ордера?\n\n${detail}`);
}

function refreshReadinessFromInputs() {
  const orderBase = Boolean(fields.symbol.value.trim() && fields.orderAmount.value.trim());
  const limitReady = fields.orderType.value !== "Limit" || Boolean(fields.orderLimitPrice.value.trim());
  const orderTyped = fields.enableTrading.checked && orderBase && limitReady;

  if (orderTyped) {
    setPill(statusNodes.order, "ready", "Ордер готов");
    ["order-rest", "order-fix", "order-ws"].forEach((name) => setCardState(name, "idle", "Готово"));
  } else if (fields.enableTrading.checked) {
    setPill(statusNodes.order, "warn", "Ордер не заполнен");
    ["order-rest", "order-fix", "order-ws"].forEach((name) => setCardState(name, "locked", "Не заполнен"));
  } else {
    setPill(statusNodes.order, "fail", "Ордер закрыт");
    ["order-rest", "order-fix", "order-ws"].forEach((name) => setCardState(name, "locked", "Закрыто"));
  }
}

function isOrderCheck(check) {
  return ["order-rest", "order-fix", "order-ws"].includes(check);
}

function isStreamOpenCheck(check) {
  return ["fix-feed-open", "ws-feed-open"].includes(check);
}

function isStreamCloseCheck(check) {
  return ["fix-feed-close", "ws-feed-close"].includes(check);
}

function cardForCheck(check) {
  if (check === "fix-feed-open" || check === "fix-feed-close") return "fix-feed";
  if (check === "ws-feed-open" || check === "ws-feed-close") return "ws-feed";
  return check;
}

function updateOrderTypeVisibility() {
  document.querySelector("#limitPriceField").classList.toggle("hidden", fields.orderType.value !== "Limit");
}

function updateHistoryTypeVisibility() {
  document.querySelector("#barsOptions").classList.toggle("hidden", fields.quoteHistoryKind.value !== "Bars");
}

function markRunning(check) {
  if (check === "rest-bundle") {
    ["public", "rest-auth", "bars"].forEach((name) => setCardState(name, "running", "Проверяю"));
    return;
  }
  setCardState(cardForCheck(check), "running", isStreamCloseCheck(check) ? "Закрываю" : "Проверяю");
}

function updateCardsAfterRun(check, results, ok) {
  if (check === "rest-bundle") {
    setCardState("public", stateForResult(results, "REST public"), labelForOk(ok, results, "REST public"));
    setCardState("rest-auth", stateForResult(results, "REST account"), labelForOk(ok, results, "REST account"));
    setCardState("bars", stateForResult(results, "Bars range"), labelForOk(ok, results, "Bars range"));
    return;
  }
  setCardState(cardForCheck(check), ok ? "pass" : "fail", ok ? (isStreamCloseCheck(check) ? "Закрыто" : "Работает") : "Ошибка");
}

function stateForResult(results, prefix) {
  const group = results.filter((item) => item.name.startsWith(prefix));
  if (!group.length) return "fail";
  return group.every((item) => item.ok) ? "pass" : "fail";
}

function labelForOk(ok, results, prefix) {
  const group = results.filter((item) => item.name.startsWith(prefix));
  if (!group.length) return "Ошибка";
  return group.every((item) => item.ok) ? "Работает" : "Ошибка";
}

function setCardState(check, state, text) {
  const card = cards.find((item) => item.dataset.card === check);
  const stateNode = document.querySelector(`[data-state-for="${check}"]`);
  if (card) {
    card.classList.remove("pass", "fail", "running");
    if (["pass", "fail", "running"].includes(state)) card.classList.add(state);
  }
  if (stateNode) {
    stateNode.classList.remove("idle", "locked", "running", "pass", "fail", "warn");
    stateNode.classList.add(state);
    stateNode.textContent = text;
  }
}

function setBusy(busy) {
  spinner.hidden = !busy;
  buttons.forEach((button) => {
    button.disabled = busy;
  });
  consoleFields.send.disabled = busy;
}

function renderResults(results) {
  if (!results.length) {
    resultList.innerHTML = `
      <div class="empty-state">
        <span class="empty-dot" aria-hidden="true"></span>
        <p>Проверка не вернула результатов.</p>
      </div>
    `;
    return;
  }

  resultList.innerHTML = results
    .map((item) => {
      const statusClass = item.ok ? "pass" : "fail";
      const statusText = item.ok ? "OK" : "FAIL";
      return `
        <div class="result-item ${statusClass}">
          <span class="result-badge">${escapeHtml(statusText)}</span>
          <div>
            <p class="result-title">${escapeHtml(item.name)}</p>
            <p class="result-detail">${escapeHtml(item.detail)}</p>
            ${item.fileUrl ? `<a class="result-file" href="${escapeHtml(item.fileUrl)}" download>${escapeHtml(item.fileName || "Скачать файл")}</a>` : ""}
          </div>
        </div>
      `;
    })
    .join("");
}

function formatRunLog(check, data) {
  const lines = [`готово: ${labels[check] || check} | ${data.ok ? "PASS" : "FAIL"} | ${data.elapsedMs || 0} ms`];
  if (data.commands?.length) {
    lines.push("  SENT:");
    for (const command of data.commands) lines.push(`    ${command}`);
  }
  for (const item of data.results || []) {
    lines.push(`  [${item.ok ? "PASS" : "FAIL"}] ${item.name}: ${item.detail}`);
  }
  return lines.join("\n");
}

function startStreamPolling(name) {
  stopStreamPolling(name, false);
  streamOffsets[name] = 0;
  pollStream(name);
  streamTimers[name] = window.setInterval(() => pollStream(name), 1200);
}

function stopStreamPolling(name, clearOffset = true) {
  if (streamTimers[name]) {
    window.clearInterval(streamTimers[name]);
    delete streamTimers[name];
  }
  if (clearOffset) delete streamOffsets[name];
}

async function pollStream(name) {
  try {
    const response = await fetch(`api/stream-status?name=${encodeURIComponent(name)}`, { cache: "no-store" });
    const data = await response.json();
    const events = data.events || [];
    const offset = streamOffsets[name] || 0;
    for (const event of events.slice(offset)) {
      const when = new Date(event.time * 1000).toLocaleTimeString();
      appendLog(`[${name}] ${when} ${event.text}`);
    }
    streamOffsets[name] = events.length;
    if (!data.running && streamTimers[name]) stopStreamPolling(name, false);
  } catch (error) {
    appendLog(`[${name}] ошибка потока: ${error.message}`);
    stopStreamPolling(name, false);
  }
}

function appendLog(message) {
  const stamp = new Date().toLocaleTimeString();
  runLog.textContent = `${runLog.textContent}${runLog.textContent ? "\n" : ""}${stamp} ${message}`;
  runLog.scrollTop = runLog.scrollHeight;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
