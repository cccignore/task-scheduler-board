(() => {
  "use strict";

  const API = Object.freeze({
    tasks: "/api/tasks",
    demo: "/api/demo",
    claim: "/api/workers/claim-next",
    create: "/api/tasks",
    logs: "/api/logs",
    workers: "/api/workers/managed",
    workersStop: "/api/workers/managed/stop",
    proofClaim: "/api/proofs/claim",
    proofIdem: "/api/proofs/idempotency",
    reset: "/api/reset",
    start: (taskId) => `/api/tasks/${taskId}/start`,
    complete: (taskId, sequence) => `/api/tasks/${taskId}/steps/${sequence}/complete`,
  });
  const POLL_MS = 1500;
  const REQUEST_TIMEOUT_MS = 10000;
  const PROOF_TIMEOUT_MS = 180000;
  const WORKER_KEY = "task-board-worker-id";
  const CLAIM_TOKEN_PREFIX = "task-board-claim-token:";
  const MAX_LOG_ROWS = 300;

  const STATUSES = ["pending", "claimed", "running", "done", "failed"];
  const STATUS_COPY = Object.freeze({
    pending: { label: "排队中", meaning: "等待工人抢单" },
    claimed: { label: "已认领", meaning: "已被唯一工人持有" },
    running: { label: "执行中", meaning: "按顺序执行工序" },
    done: { label: "已完成", meaning: "全部工序成功" },
    failed: { label: "已失败", meaning: "任一工序失败即终止" },
  });
  const STEP_STATUS = Object.freeze({
    pending: "等待执行",
    running: "执行中",
    done: "成功",
    failed: "失败",
  });
  const LOG_EVENT_DOT = Object.freeze({
    task_created: "pending",
    claim: "claimed",
    start: "running",
    step_report: "done",
    duplicate_report: "pending",
  });

  const dom = {
    connection: document.querySelector("#connectionStatus"),
    connectionText: document.querySelector("#connectionText"),
    updated: document.querySelector("#lastUpdated"),
    workerId: document.querySelector("#workerId"),
    createDemo: document.querySelector("#createDemoButton"),
    enqueue: document.querySelector("#enqueueButton"),
    claim: document.querySelector("#claimButton"),
    refresh: document.querySelector("#refreshButton"),
    reset: document.querySelector("#resetButton"),
    spawnOne: document.querySelector("#spawnOneButton"),
    spawnThree: document.querySelector("#spawnThreeButton"),
    stopWorkers: document.querySelector("#stopWorkersButton"),
    failToggle: document.querySelector("#failToggle"),
    workerChips: document.querySelector("#workerChips"),
    proofQuick: document.querySelector("#proofQuickButton"),
    proofFull: document.querySelector("#proofFullButton"),
    proofIdem: document.querySelector("#proofIdemButton"),
    proofResult: document.querySelector("#proofResult"),
    message: document.querySelector("#globalMessage"),
    summary: document.querySelector("#statusSummary"),
    kanban: document.querySelector("#kanban"),
    slots: document.querySelector("#verificationSlots"),
    verification: document.querySelector("#verificationResult"),
    logbook: document.querySelector("#logbook"),
    logFilterWarn: document.querySelector("#logFilterWarn"),
    modal: document.querySelector("#taskModal"),
    modalBody: document.querySelector("#modalBody"),
  };

  let tasks = [];
  let managedWorkers = [];
  let activeLoad = null;
  let busyAction = null;
  let lastUpdated = null;
  let renderedPayload = "";
  let renderedWorkersPayload = "";
  let boardMode = "loading";
  let openTaskId = null;
  const logState = { items: [], lastId: null, renderKey: "" };

  /* ---------- tiny DOM + format helpers ---------- */

  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = String(text);
    return node;
  }

  function valueText(value) {
    if (value === "") return '""';
    if (value === null) return "null";
    return typeof value === "object" ? JSON.stringify(value) : String(value);
  }

  function formatDate(value) {
    if (!value) return "—";
    return new Intl.DateTimeFormat("zh-CN", {
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).format(new Date(value));
  }

  function formatClock(value) {
    return new Intl.DateTimeFormat("zh-CN", {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).format(new Date(value));
  }

  function clockNow() {
    return formatClock(new Date());
  }

  /* ---------- worker id + claim tokens ---------- */

  function generatedWorkerId() {
    const suffix = crypto.randomUUID
      ? crypto.randomUUID().slice(0, 8)
      : Date.now().toString(36);
    return `worker-${suffix}`;
  }

  function loadWorkerId() {
    try {
      return localStorage.getItem(WORKER_KEY) || generatedWorkerId();
    } catch (_error) {
      return generatedWorkerId();
    }
  }

  function saveWorkerId() {
    const workerId = dom.workerId.value.trim();
    if (!workerId) return;
    try {
      localStorage.setItem(WORKER_KEY, workerId);
    } catch (_error) {
      // The input remains usable when browser storage is unavailable.
    }
  }

  function currentWorkerId() {
    const workerId = dom.workerId.value.trim();
    if (!workerId) {
      dom.workerId.focus();
      throw new Error("请先在操作台填写「我的工号」");
    }
    saveWorkerId();
    return workerId;
  }

  function saveClaimToken(taskId, claimToken) {
    try {
      sessionStorage.setItem(`${CLAIM_TOKEN_PREFIX}${taskId}`, claimToken);
    } catch (_error) {
      throw new Error("任务已认领，但当前标签页无法保存执行凭证，请允许使用 sessionStorage");
    }
  }

  function storedClaimToken(taskId) {
    try {
      return sessionStorage.getItem(`${CLAIM_TOKEN_PREFIX}${taskId}`);
    } catch (_error) {
      return null;
    }
  }

  function taskCredentials(taskId) {
    const claimToken = storedClaimToken(taskId);
    if (!claimToken) {
      throw new Error(`当前标签页没有任务 ${taskId} 的认领凭证，请在本页认领或创建演示任务`);
    }
    return { worker_id: currentWorkerId(), claim_token: claimToken };
  }

  /* ---------- HTTP ---------- */

  async function request(path, method = "GET", body = null, timeoutMs = REQUEST_TIMEOUT_MS) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(path, {
        method,
        cache: "no-store",
        headers: body === null
          ? { Accept: "application/json" }
          : { Accept: "application/json", "Content-Type": "application/json" },
        body: body === null ? undefined : JSON.stringify(body),
        signal: controller.signal,
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.detail || `请求失败（HTTP ${response.status}）`);
      return payload;
    } catch (error) {
      if (error.name === "AbortError") throw new Error("请求超时，请检查后端服务");
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  /* ---------- top-level status widgets ---------- */

  function setConnection(type, text) {
    dom.connection.className = `connection connection--${type}`;
    dom.connectionText.textContent = text;
  }

  function showMessage(text, isError = false, source = "action") {
    dom.message.textContent = text;
    dom.message.className = `notice${isError ? " notice--error" : ""}`;
    dom.message.dataset.source = source;
    dom.message.hidden = false;
  }

  function clearConnectionError() {
    if (dom.message.dataset.source === "connection") dom.message.hidden = true;
  }

  function updateTimestamp() {
    if (!lastUpdated) {
      dom.updated.textContent = "等待首次同步";
      return;
    }
    const seconds = Math.floor((Date.now() - lastUpdated.getTime()) / 1000);
    dom.updated.textContent = seconds < 2 ? "刚刚同步" : `${seconds} 秒前同步`;
  }

  /* ---------- operations logbook (server-side ledger) ---------- */

  function ingestLogs(payload) {
    if (!payload || !Array.isArray(payload.logs)) return;
    if (logState.lastId === null) {
      logState.items = payload.logs;
    } else if (payload.logs.length) {
      logState.items = payload.logs.concat(logState.items).slice(0, MAX_LOG_ROWS);
    }
    if (logState.items.length) logState.lastId = logState.items[0].id;
  }

  function renderLogbook() {
    const warnOnly = dom.logFilterWarn.checked;
    const key = `${logState.items.length ? logState.items[0].id : 0}:${logState.items.length}:${warnOnly}`;
    if (key === logState.renderKey) return;
    logState.renderKey = key;

    const rows = warnOnly
      ? logState.items.filter((entry) => entry.level === "warning")
      : logState.items;
    if (!rows.length) {
      dom.logbook.replaceChildren(
        element(
          "li",
          "logbook__empty",
          warnOnly
            ? "暂无告警记录：矛盾上报、失败与租约回收会显示在这里"
            : "台账还是空的：创建任务、启动工人后，这里会实时记录每一步",
        ),
      );
      return;
    }
    const fragment = document.createDocumentFragment();
    rows.forEach((entry) => {
      const item = element(
        "li",
        `logbook__row${entry.level === "warning" ? " logbook__row--warning" : ""}`,
      );
      const dotClass = entry.level === "warning"
        ? "failed"
        : (LOG_EVENT_DOT[entry.event] || "pending");
      const time = element("time", "", formatClock(entry.at));
      time.dateTime = entry.at;
      item.append(
        time,
        element("i", `dot status-dot-${dotClass}`),
        element("span", "", entry.message),
      );
      fragment.append(item);
    });
    dom.logbook.replaceChildren(fragment);
  }

  /* ---------- managed workers ---------- */

  function renderWorkerChips() {
    const serialized = JSON.stringify(managedWorkers);
    if (serialized === renderedWorkersPayload) return;
    renderedWorkersPayload = serialized;

    if (!managedWorkers.length) {
      dom.workerChips.replaceChildren(
        element("span", "worker-chips__empty", "当前没有在岗的模拟工人"),
      );
      return;
    }
    const fragment = document.createDocumentFragment();
    managedWorkers.forEach((worker) => {
      const chip = element("span", "chip chip--worker");
      chip.append(
        element("i", "dot status-dot-running"),
        element("span", "", `${worker.worker_id} · PID ${worker.pid}${worker.fail_rate > 0 ? " · 会失手" : ""}`),
      );
      fragment.append(chip);
    });
    fragment.append(
      element("span", "worker-chips__count", `${managedWorkers.length}/10 在岗`),
    );
    dom.workerChips.replaceChildren(fragment);
  }

  /* ---------- summary tiles ---------- */

  function renderSummary() {
    const counts = Object.fromEntries(STATUSES.map((status) => [status, 0]));
    tasks.forEach((task) => { counts[task.status] += 1; });
    const fragment = document.createDocumentFragment();
    STATUSES.forEach((status) => {
      const copy = STATUS_COPY[status];
      const card = element("article", `stat-card status-${status}`);
      const key = element("span", "stat-card__key");
      key.append(element("i", "dot"), element("span", "", `${status} · ${copy.label}`));
      card.append(
        key,
        element("strong", "", counts[status]),
        element("p", "stat-card__meaning", copy.meaning),
      );
      fragment.append(card);
    });
    dom.summary.replaceChildren(fragment);
  }

  /* ---------- kanban ---------- */

  function leaseSpan(task) {
    if (!task.lease_expires_at || !["claimed", "running"].includes(task.status)) {
      return null;
    }
    const span = element("span");
    span.dataset.leaseExpiry = task.lease_expires_at;
    applyLeaseText(span);
    return span;
  }

  function applyLeaseText(span) {
    const remaining = new Date(span.dataset.leaseExpiry).getTime() - Date.now();
    if (Number.isNaN(remaining)) {
      span.textContent = "";
      return;
    }
    if (remaining <= 0) {
      span.textContent = "开工租约已过期 · 下次认领时回收";
      span.className = "lease--expired";
      return;
    }
    const totalSeconds = Math.floor(remaining / 1000);
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = String(totalSeconds % 60).padStart(2, "0");
    span.textContent = `租约剩余 ${minutes}:${seconds}`;
    span.className = "";
  }

  function refreshLeaseTexts() {
    document.querySelectorAll("[data-lease-expiry]").forEach(applyLeaseText);
  }

  function stepDots(task) {
    const wrap = element("div", "stepdots");
    task.steps.forEach((step) => {
      const dot = element("i");
      if (step.status === "done") dot.classList.add("is-done");
      if (step.status === "failed") dot.classList.add("is-failed");
      if (step.status === "running") dot.classList.add("is-running");
      dot.title = `Step ${step.sequence} ${step.name} · ${STEP_STATUS[step.status] || step.status}`;
      wrap.append(dot);
    });
    const finished = task.steps.filter(
      (step) => step.status === "done" || step.status === "failed",
    ).length;
    wrap.append(element("span", "", `${finished}/${task.steps.length}`));
    return wrap;
  }

  function taskCard(task) {
    const isTerminal = task.status === "done" || task.status === "failed";
    const card = element(
      "li",
      `card status-${task.status}${isTerminal ? " card--terminal" : ""}`,
    );
    card.dataset.taskId = task.id;
    card.tabIndex = 0;
    card.setAttribute("role", "button");
    card.setAttribute("aria-label", `查看任务 ${task.id} 详情`);

    const top = element("div", "card__top");
    top.append(
      element("span", "card__id", `TASK #${task.id}`),
      element("span", "card__badge", `${STATUS_COPY[task.status].label}`),
    );
    card.append(top, element("h3", "card__name", task.name));

    const meta = element("div", "card__meta");
    if (task.claimed_by) meta.append(element("span", "", `⚙ ${task.claimed_by}`));
    if (task.group_name) meta.append(element("span", "", `组 · ${task.group_name}`));
    const lease = leaseSpan(task);
    if (lease) meta.append(lease);
    if (meta.childNodes.length) card.append(meta);

    card.append(stepDots(task));

    if (task.status === "running" && task.current_step) {
      card.append(
        element(
          "p",
          "card__current",
          `正在执行 Step ${task.current_step.sequence} · ${task.current_step.name}`,
        ),
      );
    } else if (task.status === "failed") {
      const failedStep = task.steps.find((step) => step.status === "failed");
      if (failedStep) {
        card.append(
          element("p", "card__current", `止步于 Step ${failedStep.sequence} · ${failedStep.name}`),
        );
      }
    }

    const token = storedClaimToken(task.id);
    if (token) {
      const actions = element("div", "card__actions");
      if (task.status === "claimed") {
        const start = element("button", "button button--primary button--small", "启动任务");
        start.type = "button";
        start.dataset.action = "start";
        start.dataset.taskId = task.id;
        start.dataset.operationKey = `start-${task.id}`;
        actions.append(start);
      }
      if (task.status === "running" && task.current_step) {
        const verify = element("button", "button button--verify button--small", "并发完成 ×5");
        verify.type = "button";
        verify.dataset.action = "complete";
        verify.dataset.taskId = task.id;
        verify.dataset.sequence = task.current_step.sequence;
        verify.dataset.operationKey = `complete-${task.id}-${task.current_step.sequence}`;
        verify.setAttribute(
          "aria-label",
          `对任务 ${task.id} 的 Step ${task.current_step.sequence} 并发发送五次完成上报`,
        );
        actions.append(verify);
      }
      if (actions.childNodes.length) card.append(actions);
    }
    return card;
  }

  function renderKanban() {
    if (!tasks.length) {
      const panel = element("div", "state-panel");
      panel.append(
        element("strong", "", "队列现在是空的"),
        element("p", "", "第 1 步：点「一键演示任务」或「往队列加 3 个任务」；第 2 步：加几名模拟工人，看任务自己流动。"),
      );
      const button = element("button", "button button--primary", "一键演示任务");
      button.type = "button";
      button.dataset.action = "empty-demo";
      panel.append(button);
      dom.kanban.replaceChildren(panel);
      dom.kanban.setAttribute("aria-busy", "false");
      return;
    }

    const fragment = document.createDocumentFragment();
    STATUSES.forEach((status) => {
      const copy = STATUS_COPY[status];
      const column = element("section", `column status-${status}`);
      column.setAttribute("aria-label", `${copy.label}任务列`);
      const header = element("div", "column__header");
      header.append(element("i", "dot"), element("span", "", status));
      header.append(element("small", "", copy.label));
      const bucket = tasks.filter((task) => task.status === status);
      header.append(element("span", "column__count", bucket.length));
      column.append(header);

      const list = element("ol", "column__list");
      if (!bucket.length) {
        list.append(element("li", "column__empty", "暂无任务"));
      } else {
        bucket.forEach((task) => list.append(taskCard(task)));
      }
      column.append(list);
      fragment.append(column);
    });
    dom.kanban.replaceChildren(fragment);
    dom.kanban.setAttribute("aria-busy", "false");
  }

  /* ---------- task detail modal ---------- */

  function parameterBlock(title, parameters, missingText) {
    const layer = element("div", "layer");
    layer.append(element("span", "layer__title", title));
    if (parameters === null || parameters === undefined) {
      layer.append(element("p", "kv--empty", missingText));
      return layer;
    }
    const entries = Object.entries(parameters);
    if (!entries.length) {
      layer.append(element("p", "kv--empty", "∅ 空字典"));
      return layer;
    }
    const list = element("dl", "kv");
    entries.forEach(([key, value]) => {
      list.append(element("dt", "", key), element("dd", "", valueText(value)));
    });
    layer.append(list);
    return layer;
  }

  function overrideRow(step) {
    const wrap = element("div", "overrides");
    const entries = Object.entries(step.overrides || {});
    if (!entries.length) {
      wrap.append(element("span", "override override--none", "无 L3 覆盖 · 完整继承当前值"));
      return wrap;
    }
    entries.forEach(([key, value]) => {
      const keep = value === "";
      const item = element("span", `override ${keep ? "override--keep" : "override--sticky"}`);
      item.append(
        element("code", "", `${key} = ${valueText(value)}`),
        element("span", "", keep ? "空串 · 保持当前值" : "覆盖 · 粘性生效"),
      );
      wrap.append(item);
    });
    return wrap;
  }

  function stepEntry(task, step) {
    const item = element("li", `step status-${step.status}`);
    item.append(element("span", "step__no", step.sequence));
    const main = element("div", "step__main");

    const topline = element("div", "step__topline");
    topline.append(
      element("h4", "", step.name),
      element("span", `step__badge badge-${step.status}`, STEP_STATUS[step.status] || step.status),
    );
    if (task.status === "running" && step.status === "running") {
      const token = storedClaimToken(task.id);
      if (token) {
        const verify = element("button", "button button--verify button--small", "并发完成 ×5");
        verify.type = "button";
        verify.dataset.action = "complete";
        verify.dataset.taskId = task.id;
        verify.dataset.sequence = step.sequence;
        verify.dataset.operationKey = `complete-${task.id}-${step.sequence}`;
        topline.append(verify);
      } else {
        topline.append(element("span", "step__log", "（本标签页无凭证，不能上报）"));
      }
    }
    main.append(topline, overrideRow(step));

    const details = element("details");
    details.dataset.stepParams = step.sequence;
    const summaryCopy = {
      pending: "生效参数（启动时预计算）",
      running: "生效参数（当前正在使用）",
      done: "生效参数（执行时快照）",
      failed: "生效参数（执行时快照）",
    }[step.status] || "生效参数";
    details.append(element("summary", "", summaryCopy));
    const resolved = step.resolved_parameters;
    if (resolved === null || resolved === undefined) {
      details.append(element("p", "kv--empty", "任务启动时解析"));
    } else {
      const entries = Object.entries(resolved);
      if (!entries.length) {
        details.append(element("p", "kv--empty", "∅ 空字典"));
      } else {
        const list = element("dl", "kv");
        entries.forEach(([key, value]) => {
          list.append(element("dt", "", key), element("dd", "", valueText(value)));
        });
        details.append(list);
      }
    }
    main.append(details);

    const log = step.execution_log;
    main.append(
      element(
        "p",
        "step__log",
        log
          ? `执行日志 #${log.id} · ${log.success ? "成功" : "失败"} · ${formatDate(log.completed_at)}（重复上报不会新增）`
          : "尚无执行日志：首次完成上报写入 1 条",
      ),
    );
    item.append(main);
    return item;
  }

  function executionLogTable(task) {
    const section = element("section");
    const title = element("h4", "section-title", "执行日志台账");
    title.append(
      element("small", "", "题目要求的幂等日志：任务 · Step 序号 · 结果 · 时间；唯一约束保证每个 Step 至多一行"),
    );
    section.append(title);

    if (!task.execution_logs.length) {
      section.append(
        element("p", "kv--empty", "尚无执行日志：running 工序的首次完成上报会写入第一行"),
      );
      return section;
    }
    const wrap = element("div", "logtable-wrap");
    const table = element("table", "logtable");
    const head = element("thead");
    const headRow = element("tr");
    ["日志 ID", "Step 序号", "结果", "完成时间"].forEach((label) => {
      headRow.append(element("th", "", label));
    });
    head.append(headRow);
    const body = element("tbody");
    task.execution_logs.forEach((log) => {
      const row = element("tr");
      row.append(
        element("td", "", `#${log.id}`),
        element("td", "", `Step ${log.step_sequence}`),
      );
      const resultCell = element("td");
      resultCell.append(
        element(
          "span",
          `step__badge badge-${log.success ? "done" : "failed"}`,
          log.success ? "成功" : "失败",
        ),
      );
      row.append(resultCell);
      const timeCell = element("td");
      const time = element("time", "", formatDate(log.completed_at));
      time.dateTime = log.completed_at;
      timeCell.append(time);
      row.append(timeCell);
      body.append(row);
    });
    table.append(head, body);
    wrap.append(table);
    section.append(wrap);
    return section;
  }

  function renderModal() {
    if (openTaskId === null) return;
    const task = tasks.find((item) => item.id === openTaskId);
    if (!task) {
      closeModal();
      return;
    }

    const openDetails = new Set(
      Array.from(dom.modalBody.querySelectorAll("details[data-step-params][open]"))
        .map((details) => details.dataset.stepParams),
    );
    const scrollTop = dom.modal.querySelector(".modal__dialog").scrollTop;

    const body = document.createDocumentFragment();

    const header = element("header", "detail__header");
    const topline = element("div", "detail__topline");
    topline.append(
      element("span", `card__badge badge-${task.status}`, `${task.status} · ${STATUS_COPY[task.status].label}`),
      element("span", "card__id", `TASK #${task.id}`),
    );
    header.append(topline, element("h3", "", task.name));
    const meta = element("div", "detail__meta");
    const metaPair = (label, value) => {
      const span = element("span", "", `${label} `);
      span.append(element("b", "", value));
      return span;
    };
    meta.append(
      metaPair("Worker", task.claimed_by || "未分配"),
      metaPair("组", task.group_name || "无"),
      metaPair("创建", formatDate(task.created_at)),
      metaPair("认领", formatDate(task.claimed_at)),
      metaPair("启动", formatDate(task.started_at)),
      metaPair("完成", formatDate(task.completed_at)),
    );
    const lease = leaseSpan(task);
    if (lease) meta.append(lease);
    header.append(meta);

    if (task.status === "claimed" || task.status === "running") {
      const token = storedClaimToken(task.id);
      const actionRow = element("div", "card__actions");
      if (task.status === "claimed" && token) {
        const start = element("button", "button button--primary", "启动任务（冻结组参数快照）");
        start.type = "button";
        start.dataset.action = "start";
        start.dataset.taskId = task.id;
        start.dataset.operationKey = `start-${task.id}`;
        actionRow.append(start);
      } else if (task.status === "claimed") {
        actionRow.append(
          element("span", "step__log", "此任务的凭证不在本标签页（可能属于模拟工人或其他页面），这里只能查看"),
        );
      }
      const requeue = element("button", "button button--danger-ghost button--small", "手动重派（回收重新排队）");
      requeue.type = "button";
      requeue.dataset.action = "requeue";
      requeue.dataset.taskId = task.id;
      requeue.dataset.operationKey = `requeue-${task.id}`;
      actionRow.append(requeue);
      header.append(actionRow);
    }
    body.append(header);

    const layerSection = element("section");
    layerSection.append(
      element("h4", "section-title", "配方三层 · 参数层级"),
    );
    const layers = element("div", "layers");
    layers.append(
      parameterBlock("L1 · Base 出厂默认", task.base_parameters, "无基础参数"),
      parameterBlock(
        "L2 · Group 快照（开工时定稿）",
        task.group_parameters_snapshot,
        task.group_id ? "任务启动时读取组参数并冻结" : "任务无所属组",
      ),
      parameterBlock("当前生效值（随工序粘性演变）", task.resolved_parameters, "任务启动后逐工序解析"),
    );
    layerSection.append(layers);
    body.append(layerSection);

    const stepSection = element("section");
    const stepTitle = element("h4", "section-title", "执行工序");
    stepTitle.append(element("small", "", `${task.steps.length} 个 Step · L3 覆盖从声明的工序起对后续粘性生效，空串保持当前值`));
    stepSection.append(stepTitle);
    const flow = element("ol", "stepflow");
    task.steps.forEach((step) => flow.append(stepEntry(task, step)));
    stepSection.append(flow);
    body.append(stepSection);

    body.append(executionLogTable(task));

    dom.modalBody.replaceChildren(body);
    openDetails.forEach((sequence) => {
      const details = dom.modalBody.querySelector(`details[data-step-params="${sequence}"]`);
      if (details) details.open = true;
    });
    dom.modal.querySelector(".modal__dialog").scrollTop = scrollTop;
    syncButtons();
  }

  function openModal(taskId) {
    openTaskId = taskId;
    dom.modal.hidden = false;
    document.body.style.overflow = "hidden";
    renderModal();
    dom.modal.querySelector(".modal__close").focus();
  }

  function closeModal() {
    openTaskId = null;
    dom.modal.hidden = true;
    document.body.style.overflow = "";
  }

  /* ---------- verification + proof panels ---------- */

  function renderSlots(states) {
    const fragment = document.createDocumentFragment();
    const labels = { idle: "—", inserted: "写入", noop: "no-op", bad: "异常" };
    states.forEach((state, index) => {
      const slot = element(
        "div",
        `slot${state === "idle" ? "" : ` slot--${state}`}`,
        `${index + 1} · ${labels[state]}`,
      );
      fragment.append(slot);
    });
    dom.slots.replaceChildren(fragment);
  }

  function showVerification(kind, title, detail) {
    dom.verification.className = `verification${kind ? ` verification--${kind}` : ""}`;
    dom.verification.replaceChildren(
      element("strong", "", title),
      element("span", "", detail),
    );
  }

  function showProofResult(kind, title, lines) {
    dom.proofResult.className = `verification${kind ? ` verification--${kind}` : ""}`;
    const children = [element("strong", "", title)];
    lines.forEach((line) => children.push(element("span", "", line)));
    dom.proofResult.replaceChildren(...children);
  }

  /* ---------- board orchestration ---------- */

  function syncButtons() {
    document.querySelectorAll("button[data-operation-key]").forEach((button) => {
      const isRefreshLoading = button.dataset.operationKey === "refresh" && activeLoad;
      button.disabled = Boolean(busyAction || isRefreshLoading);
      button.classList.toggle("is-busy", button.dataset.operationKey === busyAction);
    });
  }

  function renderBoard() {
    renderSummary();
    renderKanban();
    renderModal();
    boardMode = "ready";
    syncButtons();
  }

  function renderLoadError(error) {
    const panel = element("div", "state-panel");
    panel.append(
      element("strong", "", "暂时无法读取任务"),
      element("p", "", error.message),
    );
    const retry = element("button", "button button--primary", "重新连接");
    retry.type = "button";
    retry.dataset.action = "retry";
    panel.append(retry);
    dom.kanban.replaceChildren(panel);
    dom.kanban.setAttribute("aria-busy", "false");
    boardMode = "error";
  }

  async function loadTasks({ force = false, announce = false } = {}) {
    if (activeLoad) {
      if (!force) return activeLoad;
      await activeLoad.catch(() => undefined);
    }
    activeLoad = (async () => {
      try {
        const logsPath = logState.lastId === null
          ? `${API.logs}?limit=150`
          : `${API.logs}?after_id=${logState.lastId}&limit=200`;
        const [tasksPayload, logsPayload, workersPayload] = await Promise.all([
          request(API.tasks),
          request(logsPath).catch(() => null),
          request(API.workers).catch(() => null),
        ]);

        ingestLogs(logsPayload);
        renderLogbook();
        if (workersPayload && Array.isArray(workersPayload.workers)) {
          managedWorkers = workersPayload.workers;
          renderWorkerChips();
        }

        const serialized = JSON.stringify(tasksPayload.tasks);
        tasks = tasksPayload.tasks;
        if (serialized !== renderedPayload || boardMode !== "ready") {
          renderedPayload = serialized;
          renderBoard();
        }
        lastUpdated = new Date();
        updateTimestamp();
        setConnection("online", "轮询正常 · 1.5s");
        clearConnectionError();
        if (announce) showMessage("任务状态已刷新");
        return tasks;
      } catch (error) {
        setConnection("error", "连接异常");
        showMessage(`同步失败：${error.message}`, true, "connection");
        if (!tasks.length) renderLoadError(error);
        throw error;
      } finally {
        activeLoad = null;
        syncButtons();
      }
    })();
    syncButtons();
    return activeLoad;
  }

  async function runAction(key, operation, successMessage) {
    if (busyAction) return;
    busyAction = key;
    syncButtons();
    try {
      const payload = await operation();
      showMessage(successMessage(payload));
      await loadTasks({ force: true });
    } catch (error) {
      showMessage(`操作失败：${error.message}`, true);
    } finally {
      busyAction = null;
      syncButtons();
    }
  }

  /* ---------- the ×5 idempotency demonstration ---------- */

  async function completeFiveTimes(taskId, sequence) {
    const key = `complete-${taskId}-${sequence}`;
    if (busyAction) return;
    busyAction = key;
    syncButtons();
    try {
      const credentials = taskCredentials(taskId);
      renderSlots(["idle", "idle", "idle", "idle", "idle"]);
      showVerification("", "正在并发上报…", "五个相同的完成请求已经同时发出");
      const requests = Array.from({ length: 5 }, () => request(
        API.complete(taskId, sequence),
        "POST",
        { ...credentials, success: true },
      ));
      const settled = await Promise.allSettled(requests);
      const fulfilled = settled
        .filter((result) => result.status === "fulfilled")
        .map((result) => result.value);
      const inserted = fulfilled.filter(
        (payload) => payload.inserted === true && payload.duplicate === false,
      ).length;
      const duplicates = fulfilled.filter(
        (payload) => payload.inserted === false && payload.duplicate === true,
      ).length;
      const invalid = fulfilled.length - inserted - duplicates;
      const rejected = settled.length - fulfilled.length;
      await loadTasks({ force: true });

      const task = tasks.find((item) => item.id === Number(taskId));
      const logCount = task
        ? task.execution_logs.filter((log) => log.step_sequence === Number(sequence)).length
        : null;

      const slotStates = [];
      for (let index = 0; index < 5; index += 1) {
        if (index < inserted) slotStates.push("inserted");
        else if (index < inserted + duplicates) slotStates.push("noop");
        else slotStates.push("bad");
      }
      renderSlots(slotStates);

      if (inserted === 1 && duplicates === 4 && invalid === 0 && rejected === 0 && logCount === 1) {
        showVerification(
          "pass",
          "幂等验证通过 · 1 次写入 + 4 次 no-op",
          `任务 #${taskId} Step ${sequence}：五个响应全部返回，账本该工序仍只有 1 行；4 次 no-op 已记入下方系统台账`,
        );
        showMessage(`任务 ${taskId} / Step ${sequence}：幂等写入验证通过`);
      } else {
        showVerification(
          "error",
          "验证结果异常",
          `写入 ${inserted} · no-op ${duplicates} · 无效 ${invalid} · 失败 ${rejected}；DB ${logCount === null ? "未读到" : `${logCount} 行`}`,
        );
        showMessage("并发验证未达到预期，请查看系统台账", true);
      }
    } catch (error) {
      renderSlots(["bad", "bad", "bad", "bad", "bad"]);
      showVerification("error", "验证失败", error.message);
      showMessage(`并发验证失败：${error.message}`, true);
    } finally {
      busyAction = null;
      syncButtons();
    }
  }

  /* ---------- proofs ---------- */

  async function runProof(key, path, body, runningTitle) {
    if (busyAction) return;
    busyAction = key;
    syncButtons();
    showProofResult("", runningTitle, ["真实 spawn 子进程正在临时数据库上竞争，请稍候几秒…"]);
    try {
      const payload = await request(path, "POST", body, PROOF_TIMEOUT_MS);
      const stats = payload.stats;
      if (payload.kind === "claim") {
        showProofResult(
          stats.passed ? "pass" : "error",
          stats.passed ? "并发认领证明 PASS" : "并发认领证明 FAIL",
          [
            `${stats.workers} 个真实进程 × ${stats.rounds} 轮 = ${stats.claim_attempts} 次同时认领`,
            `重复认领 ${stats.duplicate_claims} 次 · 每轮唯一赢家 ${stats.unique_winners}/${stats.rounds} · 启动方式 ${stats.start_method}`,
            "全部进程各自持有独立数据库连接，赛后核对数据库终态与凭证唯一性",
          ],
        );
      } else {
        showProofResult(
          stats.passed ? "pass" : "error",
          stats.passed ? "幂等写入证明 PASS" : "幂等写入证明 FAIL",
          [
            `5 个真实进程同时上报同一工序 → ${stats.inserted_responses} 次写入 + ${stats.duplicate_responses} 次 no-op`,
            `账本行数 ${stats.log_rows} · 成功后补报失败无效 ${stats.late_failure_noop ? "✓" : "✗"} · 失败后补报成功无效 ${stats.failure_first_noop ? "✓" : "✗"}`,
          ],
        );
      }
      showMessage("多进程证明运行完成");
    } catch (error) {
      showProofResult("error", "证明运行失败", [error.message]);
      showMessage(`证明运行失败：${error.message}`, true);
    } finally {
      busyAction = null;
      syncButtons();
    }
  }

  /* ---------- queue seeding ---------- */

  function demoTaskPayloads() {
    const stamp = clockNow().replace(/:/g, "");
    return [
      {
        name: `广播通知 ${stamp}-A`,
        base_parameters: { channel: "sms", retries: 1, locale: "zh-CN" },
        steps: [
          { name: "准备收件人", overrides: { batch: 50 } },
          { name: "发送消息", overrides: { channel: "push", batch: "" } },
          { name: "回执统计", overrides: { retries: 3 } },
        ],
      },
      {
        name: `账单提醒 ${stamp}-B`,
        base_parameters: { channel: "email", template: "bill-v2" },
        steps: [
          { name: "渲染模板", overrides: {} },
          { name: "批量发送", overrides: { template: "" } },
        ],
      },
      {
        name: `回访跟进 ${stamp}-C`,
        base_parameters: { channel: "call" },
        steps: [{ name: "外呼", overrides: {} }],
      },
    ];
  }

  /* ---------- wiring ---------- */

  dom.workerId.value = loadWorkerId();
  dom.workerId.addEventListener("change", saveWorkerId);
  dom.workerId.addEventListener("blur", saveWorkerId);
  dom.logFilterWarn.addEventListener("change", renderLogbook);

  dom.refresh.addEventListener("click", () => {
    loadTasks({ force: true, announce: true }).catch(() => undefined);
  });

  dom.createDemo.addEventListener("click", () => {
    runAction(
      "create",
      () => request(API.demo, "POST"),
      (payload) => {
        saveClaimToken(payload.task.id, payload.claim_token);
        dom.workerId.value = payload.task.claimed_by;
        saveWorkerId();
        return `演示任务 #${payload.task.id} 已创建并进入执行中，另有一个排队任务；点它的卡片看配方，点「并发完成 ×5」验证幂等`;
      },
    );
  });

  dom.enqueue.addEventListener("click", () => {
    runAction(
      "enqueue",
      async () => {
        const created = [];
        for (const body of demoTaskPayloads()) {
          const payload = await request(API.create, "POST", body);
          created.push(payload.task.id);
        }
        return created;
      },
      (ids) => `已创建 ${ids.length} 个排队任务（#${ids.join(" #")}）；加几名模拟工人它们就会被抢走`,
    );
  });

  dom.claim.addEventListener("click", () => {
    runAction(
      "claim",
      () => request(API.claim, "POST", { worker_id: currentWorkerId() }),
      (payload) => {
        if (!payload.task) return "当前没有可认领的任务，先造几个任务";
        saveClaimToken(payload.task.id, payload.claim_token);
        return `已认领任务 #${payload.task.id}，凭证已存入本标签页；在卡片上点「启动任务」`;
      },
    );
  });

  dom.spawnOne.addEventListener("click", () => {
    runAction(
      "spawn-1",
      () => request(API.workers, "POST", {
        count: 1,
        step_seconds: 1.2,
        fail_rate: dom.failToggle.checked ? 0.15 : 0,
      }),
      (payload) => `已上岗 1 名模拟工人（现在 ${payload.workers.length} 名在岗），它们是真实的独立进程`,
    );
  });

  dom.spawnThree.addEventListener("click", () => {
    runAction(
      "spawn-3",
      () => request(API.workers, "POST", {
        count: 3,
        step_seconds: 1.2,
        fail_rate: dom.failToggle.checked ? 0.15 : 0,
      }),
      (payload) => `已上岗 3 名模拟工人（现在 ${payload.workers.length} 名在岗），看它们在台账里抢任务`,
    );
  });

  dom.stopWorkers.addEventListener("click", () => {
    runAction(
      "stop-workers",
      () => request(API.workersStop, "POST"),
      (payload) => payload.stopped
        ? `${payload.stopped} 名模拟工人已下班；执行到一半的任务会在租约到期后被回收重派`
        : "当前没有在岗的模拟工人",
    );
  });

  dom.reset.addEventListener("click", () => {
    const confirmed = window.confirm(
      "确定清空看板吗？\n所有任务、执行日志、组和系统台账都会删除，在岗模拟工人会被停止。",
    );
    if (!confirmed) return;
    runAction(
      "reset",
      async () => {
        const payload = await request(API.reset, "POST");
        logState.items = [];
        logState.lastId = null;
        logState.renderKey = "";
        renderSlots(["idle", "idle", "idle", "idle", "idle"]);
        showVerification("", "等待验证", "先创建演示任务，再在执行中工序上点「并发完成 ×5」");
        return payload;
      },
      (payload) => `看板已清空${payload.stopped_workers ? `，并停止了 ${payload.stopped_workers} 名模拟工人` : ""}`,
    );
  });

  dom.proofQuick.addEventListener("click", () => {
    runProof("proof-quick", API.proofClaim, { rounds: 12, workers: 6 }, "并发认领证明运行中…");
  });
  dom.proofFull.addEventListener("click", () => {
    runProof("proof-full", API.proofClaim, { rounds: 40, workers: 8 }, "完整并发认领证明运行中…");
  });
  dom.proofIdem.addEventListener("click", () => {
    runProof("proof-idem", API.proofIdem, null, "幂等写入证明运行中…");
  });

  document.addEventListener("click", (event) => {
    const closer = event.target.closest("[data-modal-close]");
    if (closer) {
      closeModal();
      return;
    }
    const button = event.target.closest("button[data-action]");
    if (button) {
      const { action } = button.dataset;
      if (action === "retry") {
        loadTasks({ force: true }).catch(() => undefined);
      } else if (action === "empty-demo") {
        dom.createDemo.click();
      } else if (action === "start") {
        const taskId = Number(button.dataset.taskId);
        runAction(
          `start-${taskId}`,
          () => request(API.start(taskId), "POST", taskCredentials(taskId)),
          () => `任务 #${taskId} 已启动：组参数已定稿，全部工序的生效配方已解析`,
        );
      } else if (action === "requeue") {
        const taskId = Number(button.dataset.taskId);
        const confirmed = window.confirm(
          `确定手动重派任务 #${taskId} 吗？\n持有者凭证将作废，任务回到排队中；已完成 Step 的日志保留。\n注意：若原工人已执行过外部动作（如已发出消息），重派可能造成重复执行——这正是系统不自动回收已开工任务的原因。`,
        );
        if (!confirmed) return;
        runAction(
          `requeue-${taskId}`,
          () => request(`/api/tasks/${taskId}/requeue`, "POST"),
          () => `任务 #${taskId} 已回收重新排队；下一个认领者将从第一个未完成 Step 续跑`,
        );
      } else if (action === "complete") {
        completeFiveTimes(button.dataset.taskId, button.dataset.sequence);
      }
      return;
    }
    const card = event.target.closest(".card[data-task-id]");
    if (card) openModal(Number(card.dataset.taskId));
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !dom.modal.hidden) {
      closeModal();
      return;
    }
    if ((event.key === "Enter" || event.key === " ") && event.target.matches(".card[data-task-id]")) {
      event.preventDefault();
      openModal(Number(event.target.dataset.taskId));
    }
  });

  renderSummary();
  renderSlots(["idle", "idle", "idle", "idle", "idle"]);
  renderLogbook();
  renderWorkerChips();
  dom.kanban.replaceChildren(
    (() => {
      const panel = element("div", "state-panel");
      panel.append(element("strong", "", "正在读取任务"), element("p", "", "正在连接本地调度服务…"));
      return panel;
    })(),
  );
  loadTasks().catch(() => undefined);
  setInterval(() => loadTasks().catch(() => undefined), POLL_MS);
  setInterval(() => {
    updateTimestamp();
    refreshLeaseTexts();
  }, 1000);
})();
