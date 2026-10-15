import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import ts from "typescript";
import { en } from "../i18n/messages/settings.ts";

const defaults = {
  enabled: false,
  archive_after_days: 7,
  delete_after_archive_days: 30,
  check_interval_hours: 24,
  batch_size: 50,
  cycle_timeout_seconds: 120,
  revision: "fixture-revision",
  source: "environment",
  scheduler_available: true,
  scheduler_running: false,
  next_check_unix: null,
};

function mount(overrides = {}) {
  const state = [], refs = [], effects = [], pending = [];
  let cursor = 0, tree;
  const calls = [], notices = [];
  const api = {
    taskRetention: async () => ({ ...defaults }),
    saveTaskRetention: async (input) => { calls.push(input); return { ...defaults, ...input, source: "saved" }; },
    ...overrides,
  };
  const react = {
    useState(initial) {
      const i = cursor++;
      if (!(i in state)) state[i] = typeof initial === "function" ? initial() : initial;
      return [state[i], (next) => { state[i] = typeof next === "function" ? next(state[i]) : next; }];
    },
    useRef(initial) { const i = cursor++; refs[i] ??= { current: initial }; return refs[i]; },
    useEffect(fn, deps) {
      const i = cursor++;
      if (!effects[i] || deps.some((d, n) => d !== effects[i].deps[n])) {
        effects[i]?.cleanup?.();
        effects[i] = { deps };
        pending.push(() => { effects[i].cleanup = fn(); });
      }
    },
  };
  const jsx = (type, props) => ({ type, props });
  const i18n = { t: (key, values = {}) => (en[key.replace("settings.", "")] ?? key).replace(/\{(\w+)\}/g, (_, k) => String(values[k])), fmt: { dateTime: (value) => String(value) } };
  const module = { exports: {} };
  const source = readFileSync(new URL("../components/task-retention-settings.tsx", import.meta.url), "utf8");
  const load = (source, extra = {}) => {
    const m = { exports: {} };
    vm.runInNewContext(ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX } }).outputText, {
      module: m, exports: m.exports, Date, Number, JSON, Error,
      require(name) {
        if (name === "react") return react;
        if (name === "react/jsx-runtime") return { jsx, jsxs: jsx, Fragment: "fragment" };
        if (name === "@/lib/api") return { api };
        if (name === "sonner") return { toast: { success: (s) => notices.push(s) } };
        if (name === "@/i18n") return { useI18n: () => i18n };
        if (extra[name]) return extra[name];
        return new Proxy({}, { get: (_, key) => key });
      },
    });
    return m.exports;
  };
  const helpers = load(readFileSync(new URL("./task-retention.ts", import.meta.url), "utf8"));
  Object.assign(module.exports, load(source, { "@/lib/task-retention": helpers }));
  const Component = module.exports.TaskRetentionSettings;
  assert.equal(typeof Component, "function");
  function render() {
    cursor = 0;
    tree = Component();
    while (pending.length) pending.shift()();
    return tree;
  }
  function nodes(node = tree) {
    if (!node || typeof node !== "object") return [];
    return [node, ...[node.props?.children].flat(Infinity).filter((child) => child !== undefined).flatMap(nodes)];
  }
  render();
  return {
    calls, notices, api, render,
    byId: (id) => nodes().find((n) => n.props?.id === id),
    byType: (type) => nodes().find((n) => n.type === type),
    text: () => JSON.stringify(tree),
    async open() { nodes().find((n) => n.type === "Dialog").props.onOpenChange(true); render(); await this.flush(); },
    async flush() { await new Promise(setImmediate); render(); },
    async change(id, value) { this.byId(id).props.onChange({ target: { value } }); render(); },
  };
}

test("retention dialog sits next to concurrency, loads effective values without saving", async () => {
  const page = readFileSync(new URL("../app/(main)/function/tasks/page.tsx", import.meta.url), "utf8");
  assert.match(page, /<ConcurrencySettingsDialog\s*\/>\s*<TaskRetentionSettings\s*\/>/);
  const ui = mount();
  await ui.open();
  assert.equal(ui.byId("retention-archive-days").props.value, "7");
  assert.equal(ui.byId("retention-delete-days").props.value, "30");
  assert.equal(ui.byId("retention-interval-hours").props.value, "24");
  assert.equal(ui.byId("retention-enabled").props.checked, false);
  assert.equal(ui.calls.length, 0);
  assert.match(ui.text(), /37/);
});

test("disabled settings save uses loaded revision and never enables cleanup", async () => {
  const ui = mount();
  await ui.open();
  await ui.change("retention-archive-days", "10");
  await ui.byId("retention-save").props.onClick();
  await ui.flush();
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.calls[0].archive_after_days, 10);
  assert.equal(ui.calls[0].enabled, false);
  assert.equal(ui.calls[0].revision, defaults.revision);
  assert.equal(ui.calls[0].confirm_enable, false);
});

test("enabled saves require separate confirmation; cancel does not write", async () => {
  const ui = mount();
  await ui.open();
  ui.byId("retention-enabled").props.onCheckedChange(true);
  ui.render();
  await ui.byId("retention-save").props.onClick();
  ui.render();
  assert.equal(ui.calls.length, 0);
  assert.equal(ui.byType("AlertDialog").props.open, true);
  ui.byId("retention-confirm-cancel").props.onClick();
  ui.render();
  assert.equal(ui.calls.length, 0);
  await ui.byId("retention-save").props.onClick();
  ui.render();
  await ui.byId("retention-confirm-enable").props.onClick({ preventDefault() {} });
  await ui.flush();
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.calls[0].enabled, true);
  assert.equal(ui.calls[0].confirm_enable, true);
});

test("load failure prevents a defaults-based save", async () => {
  const ui = mount({ taskRetention: async () => { throw new Error("fixture read failure"); } });
  await ui.open();
  assert.equal(ui.byId("retention-save").props.disabled, true);
  assert.match(ui.text(), /fixture read failure/);
  assert.equal(ui.calls.length, 0);
});

test("invalid days and fractions cannot reach the save API", async () => {
  const ui = mount();
  await ui.open();
  for (const value of ["", "0", "-1", "1.5", "3651", "1e2"]) {
    await ui.change("retention-delete-days", value);
    await ui.byId("retention-save").props.onClick();
    ui.render();
    assert.equal(ui.calls.length, 0);
  }
});

test("save failure keeps the dialog open and requires reload before retry", async () => {
  const ui = mount({ saveTaskRetention: async () => { throw new Error("fixture save conflict"); } });
  await ui.open();
  await ui.byId("retention-save").props.onClick();
  await ui.flush();
  assert.match(ui.text(), /fixture save conflict/);
  assert.equal(ui.byId("retention-save").props.disabled, true);
});

test("closing confirmation never sends a write and reopening restores saved values", async () => {
  const ui = mount();
  await ui.open();
  await ui.change("retention-archive-days", "15");
  ui.byType("Dialog").props.onOpenChange(false);
  ui.render();
  await ui.open();
  assert.equal(ui.byId("retention-archive-days").props.value, "7");
  assert.equal(ui.calls.length, 0);
});

test("repeated save clicks send one request and disable inputs in flight", async () => {
  let finish;
  const requests = [];
  const ui = mount({ saveTaskRetention: (input) => {
    requests.push(input);
    return new Promise((resolve) => { finish = () => resolve({ ...defaults, ...input, source: "saved" }); });
  } });
  await ui.open();
  const first = ui.byId("retention-save").props.onClick();
  const second = ui.byId("retention-save").props.onClick();
  ui.render();
  assert.equal(requests.length, 1);
  assert.equal(ui.byId("retention-save").props.disabled, true);
  assert.equal(ui.byId("retention-archive-days").props.disabled, true);
  finish();
  await Promise.all([first, second]);
});

test("API helpers use authenticated same-origin retention GET/PUT without retry", async () => {
  const requests = [];
  const module = { exports: {} };
  const source = readFileSync(new URL("./api.ts", import.meta.url), "utf8");
  vm.runInNewContext(ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS } }).outputText, {
    module, exports: module.exports, window: {},
    localStorage: { getItem: () => "fixture-only-admin-jwt" },
    require(name) {
      if (name === "@/i18n/request") return { localizedFetch: async (url, init) => {
        requests.push({ url, init });
        return { ok: true, status: 200, json: async () => ({ ...defaults }) };
      } };
      if (name === "@/lib/mock/enabled") return { MOCK: false };
      return {};
    },
  });
  await module.exports.api.taskRetention();
  const input = { ...defaults, confirm_enable: false };
  await module.exports.api.saveTaskRetention(input);
  assert.equal(requests.length, 2);
  assert.equal(requests[0].url, "/api/settings/task-retention");
  assert.equal(requests[0].init.cache, "no-store");
  assert.equal(requests[1].init.method, "PUT");
  assert.equal(requests[1].init.headers.Authorization, "Bearer fixture-only-admin-jwt");
  assert.deepEqual(JSON.parse(requests[1].init.body), input);
});
