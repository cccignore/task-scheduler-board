(() => {
  "use strict";

  const API = Object.freeze({
    tasks: "/api/tasks",
    demo: "/api/demo",
    claim: "/api/workers/claim-next",
    start: (taskId) => `/api/tasks/${taskId}/start`,
    complete: (taskId, sequence) => `/api/tasks/${taskId}/steps/${sequence}/complete`,
  });
  const POLL_MS = 1500;
  const REQUEST_TIMEOUT_MS = 10000;
  const WORKER_KEY = "task-board-worker-id";
  const CLAIM_TOKEN_PREFIX = "task-board-claim-token:";
  const STATUSES = ["pending", "claimed", "running", "done", "failed"];

  const dom = {
    connection: document.querySelector("#connectionStatus"),
    connectionText: document.querySelector("#connectionText"),
    workerId: document.querySelector("#workerId"),
    createDemo: document.querySelector("#createDemoButton"),
    claim: document.querySelector("#claimButton"),
    refresh: document.querySelector("#refreshButton"),
    summary: document.querySelector("#statusSummary"),
    updated: document.querySelector("#lastUpdated"),
    message: document.querySelector("#globalMessage"),
    taskList: document.querySelector("#taskList"),
    verification: document.querySelector("#verificationResult"),
  };

  let tasks = [];
  let activeLoad = null;
  let busyAction = null;
  let lastUpdated = null;
  let renderedPayload = "";
  let boardMode = "loading";

  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = String(text);
    return node;
  }

  function valueText(value) {
    if (value === "") return "\"\"";
    if (value === null) return "null";
    return typeof value === "object" ? JSON.stringify(value) : String(value);
  }

  function parameterList(parameters, missingText) {
    if (parameters === null) return element("p", "parameter-empty", missingText);
    const entries = Object.entries(parameters);
    if (!entries.length) return element("p", "parameter-empty", "∅ 无参数");
    const list = element("dl", "parameter-list");
    entries.forEach(([key, value]) => {
      list.append(element("dt", "", key), element("dd", "", valueText(value)));
    });
    return list;
  }

  function inlineParameters(parameters) {
    const entries = Object.entries(parameters);
    if (!entries.length) return "无";
    return entries.map(([key, value]) => `${key}=${valueText(value)}`).join(" · ");
  }

  function chip(parent, text, className = "chip") {
    if (text !== null) parent.append(element("span", className, text));
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

  function generatedWorkerId() {
    const suffix = crypto.randomUUID ? crypto.randomUUID().slice(0, 8) : Date.now().toString(36);
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
      throw new Error("请先填写 Worker ID");
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

  function taskCredentials(taskId) {
    let claimToken = null;
    try {
      claimToken = sessionStorage.getItem(`${CLAIM_TOKEN_PREFIX}${taskId}`);
    } catch (_error) {
      throw new Error("当前标签页无法读取任务执行凭证，请允许使用 sessionStorage");
    }
    if (!claimToken) {
      throw new Error(`当前标签页没有任务 ${taskId} 的认领凭证，请在本页重新认领或创建演示任务`);
    }
    return { worker_id: currentWorkerId(), claim_token: claimToken };
  }

  async function request(path, method = "GET", body = null) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
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

  function showVerification(title, detail, isError = false) {
    dom.verification.className = `verification__result${isError ? " verification__result--error" : ""}`;
    dom.verification.replaceChildren(
      element("strong", "", title),
      element("span", "", detail),
    );
  }

  function syncButtons() {
    document.querySelectorAll("button[data-operation-key]").forEach((button) => {
      const isRefreshLoading = button.dataset.operationKey === "refresh" && activeLoad;
      button.disabled = Boolean(busyAction || isRefreshLoading);
      button.classList.toggle("is-busy", button.dataset.operationKey === busyAction);
    });
  }

  function updateTimestamp() {
    if (!lastUpdated) {
      dom.updated.textContent = "等待首次同步";
      return;
    }
    const seconds = Math.floor((Date.now() - lastUpdated.getTime()) / 1000);
    dom.updated.textContent = seconds < 2 ? "刚刚同步" : `${seconds} 秒前同步 · 自动轮询中`;
  }

  function renderSummary() {
    const counts = Object.fromEntries(STATUSES.map((status) => [status, 0]));
    tasks.forEach((task) => { counts[task.status] += 1; });
    const fragment = document.createDocumentFragment();
    STATUSES.forEach((status) => {
      const card = element("article", `stat-card status-${status}`);
      card.append(element("span", "", status), element("strong", "", counts[status]));
      fragment.append(card);
    });
    dom.summary.replaceChildren(fragment);
  }

  function parameterDrawer(task) {
    const details = element("details", "parameter-drawer");
    details.dataset.taskParameters = task.id;
    details.append(element("summary", "", "参数层级 · L1 / L2 / 当前生效值"));
    const layers = element("div", "parameter-layers");
    [
      ["L1 · base", task.base_parameters, "无基础参数"],
      ["L2 · group snapshot", task.group_parameters_snapshot, "任务启动时冻结"],
      ["当前生效值", task.resolved_parameters, "任务启动时解析"],
    ].forEach(([label, parameters, missing]) => {
      const layer = element("div", "parameter-layer");
      layer.append(element("span", "parameter-label", label), parameterList(parameters, missing));
      layers.append(layer);
    });
    details.append(layers);
    return details;
  }

  function stepItem(task, step) {
    const item = element("li", `step status-${step.status}`);
    item.append(element("div", "step__number", step.sequence));

    const content = element("div", "step__content");
    const topline = element("div", "step__topline");
    topline.append(element("h5", "", step.name), element("span", "badge", step.status));
    content.append(topline);
    content.append(element("p", "step__override", `L3 override · ${inlineParameters(step.overrides)}`));

    const resolved = element("div", "resolved-block");
    resolved.append(
      element("span", "parameter-label", "Resolved parameters · 最终参数"),
      parameterList(step.resolved_parameters, "任务启动时解析"),
    );
    content.append(resolved);

    const logs = task.execution_logs.filter((log) => log.step_sequence === step.sequence);
    const logRow = element("div", "step__logs");
    chip(logRow, `执行日志 ${logs.length} 条`);
    if (logs.length) chip(logRow, logs[0].success ? "结果：成功" : "结果：失败");
    content.append(logRow);
    item.append(content);

    if (step.status === "running") {
      const action = element("div", "step__action");
      const button = element("button", "button button--danger button--small", "并发完成 ×5");
      button.type = "button";
      button.dataset.action = "complete";
      button.dataset.taskId = task.id;
      button.dataset.sequence = step.sequence;
      button.dataset.operationKey = `complete-${task.id}-${step.sequence}`;
      button.setAttribute("aria-label", `对任务 ${task.id} 的步骤 ${step.sequence} 并发完成五次`);
      action.append(button);
      item.append(action);
    }
    return item;
  }

  function taskCard(task) {
    const card = element("article", `task-card status-${task.status}`);
    const header = element("div", "task-card__header");
    const identity = element("div", "task-card__identity");
    const topline = element("div", "task-card__topline");
    topline.append(
      element("span", "task-id", `TASK / ${task.id}`),
      element("span", "badge", task.status),
    );
    identity.append(topline, element("h3", "", task.name));
    const meta = element("div", "task-meta");
    chip(meta, task.claimed_by ? `Worker · ${task.claimed_by}` : "尚未分配 Worker");
    chip(meta, task.group_name ? `Group · ${task.group_name}` : null);
    chip(meta, task.current_step ? `当前 Step ${task.current_step.sequence}` : null);
    chip(meta, `更新 · ${formatDate(task.updated_at)}`);
    identity.append(meta);
    header.append(identity);

    const finished = task.steps.filter((step) => step.status === "done" || step.status === "failed").length;
    const percent = Math.round((finished / task.steps.length) * 100);
    const side = element("div", "task-card__side");
    side.append(element("span", "progress-copy", `步骤进度 ${finished} / ${task.steps.length}`));
    const progress = element("div", "progress");
    progress.setAttribute("role", "progressbar");
    progress.setAttribute("aria-valuemin", "0");
    progress.setAttribute("aria-valuemax", "100");
    progress.setAttribute("aria-valuenow", percent);
    const fill = element("span");
    fill.style.width = `${percent}%`;
    progress.append(fill);
    side.append(progress);

    if (task.status === "claimed") {
      const start = element("button", "button button--primary button--small", "启动任务");
      start.type = "button";
      start.dataset.action = "start";
      start.dataset.taskId = task.id;
      start.dataset.operationKey = `start-${task.id}`;
      side.append(start);
    }
    header.append(side);
    card.append(header, parameterDrawer(task));

    const stepsWrap = element("div", "steps-wrap");
    const heading = element("div", "steps-heading");
    heading.append(element("h4", "", "执行步骤"), element("span", "task-id", `${task.steps.length} STEPS`));
    const steps = element("ol", "steps");
    task.steps.forEach((step) => steps.append(stepItem(task, step)));
    stepsWrap.append(heading, steps);
    card.append(stepsWrap);
    return card;
  }

  function statePanel(title, detail, withAction = false) {
    const panel = element("div", "state-panel");
    const content = element("div");
    content.append(element("strong", "", title), element("p", "", detail));
    if (withAction) {
      const button = element("button", "button button--primary", "创建演示任务");
      button.type = "button";
      button.dataset.action = "empty-demo";
      content.append(button);
    }
    panel.append(content);
    return panel;
  }

  function renderBoard() {
    const openDrawers = new Set(
      Array.from(document.querySelectorAll("details[data-task-parameters][open]"))
        .map((details) => details.dataset.taskParameters),
    );
    renderSummary();
    if (!tasks.length) {
      dom.taskList.replaceChildren(statePanel("队列现在是空的", "创建演示任务即可走通认领、启动与完成流程。", true));
    } else {
      const fragment = document.createDocumentFragment();
      tasks.forEach((task) => fragment.append(taskCard(task)));
      dom.taskList.replaceChildren(fragment);
      openDrawers.forEach((taskId) => {
        const drawer = Array.from(document.querySelectorAll("details[data-task-parameters]"))
          .find((details) => details.dataset.taskParameters === taskId);
        if (drawer) drawer.open = true;
      });
    }
    boardMode = "ready";
    dom.taskList.setAttribute("aria-busy", "false");
    syncButtons();
  }

  function renderLoading() {
    dom.taskList.replaceChildren(statePanel("正在读取任务", "正在连接本地调度服务…"));
    dom.taskList.setAttribute("aria-busy", "true");
  }

  function renderLoadError(error) {
    const panel = statePanel("暂时无法读取任务", error.message);
    const retry = element("button", "button button--primary", "重新连接");
    retry.type = "button";
    retry.dataset.action = "retry";
    panel.firstElementChild.append(retry);
    dom.taskList.replaceChildren(panel);
    dom.taskList.setAttribute("aria-busy", "false");
    boardMode = "error";
  }

  async function loadTasks({ force = false, announce = false } = {}) {
    if (activeLoad) {
      if (!force) return activeLoad;
      await activeLoad.catch(() => undefined);
    }
    activeLoad = (async () => {
      try {
        const payload = await request(API.tasks);
        const serialized = JSON.stringify(payload.tasks);
        tasks = payload.tasks;
        if (serialized !== renderedPayload || boardMode !== "ready") {
          renderedPayload = serialized;
          renderBoard();
        }
        lastUpdated = new Date();
        updateTimestamp();
        setConnection("online", "实时连接 · 1.5s");
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

  async function completeFiveTimes(taskId, sequence) {
    const key = `complete-${taskId}-${sequence}`;
    if (busyAction) return;
    busyAction = key;
    syncButtons();
    try {
      const credentials = taskCredentials(taskId);
      showVerification("正在并发上报", "五个相同请求已经同时发出");
      const requests = Array.from({ length: 5 }, () => request(
        API.complete(taskId, sequence),
        "POST",
        { ...credentials, success: true },
      ));
      const settled = await Promise.allSettled(requests);
      const fulfilled = settled.filter((result) => result.status === "fulfilled").length;
      await loadTasks({ force: true });

      const task = tasks.find((item) => item.id === Number(taskId));
      const logCount = task.execution_logs.filter((log) => log.step_sequence === Number(sequence)).length;
      if (fulfilled === 5 && logCount === 1) {
        showVerification("幂等验证通过", "5/5 次请求成功，数据库中该步骤仍只有 1 条执行日志");
        showMessage(`任务 ${taskId} / Step ${sequence}：并发上报成功，日志保持 1 条`);
      } else {
        showVerification(
          "验证结果异常",
          `${fulfilled}/5 次请求成功，实际日志 ${logCount} 条`,
          true,
        );
        showMessage("并发验证未达到预期，请查看服务日志", true);
      }
    } catch (error) {
      showVerification("验证失败", error.message, true);
      showMessage(`并发验证失败：${error.message}`, true);
    } finally {
      busyAction = null;
      syncButtons();
    }
  }

  dom.workerId.value = loadWorkerId();
  dom.workerId.addEventListener("change", saveWorkerId);
  dom.workerId.addEventListener("blur", saveWorkerId);
  dom.refresh.addEventListener("click", () => loadTasks({ force: true, announce: true }).catch(() => undefined));
  dom.createDemo.addEventListener("click", () => {
    runAction(
      "create",
      () => request(API.demo, "POST"),
      (payload) => {
        saveClaimToken(payload.task.id, payload.claim_token);
        dom.workerId.value = payload.task.claimed_by;
        saveWorkerId();
        return `演示任务 ${payload.task.id} 已创建，Worker 已切换为 ${payload.task.claimed_by}`;
      },
    );
  });
  dom.claim.addEventListener("click", () => {
    runAction(
      "claim",
      () => request(API.claim, "POST", { worker_id: currentWorkerId() }),
      (payload) => {
        if (!payload.task) return "当前没有可认领的任务";
        saveClaimToken(payload.task.id, payload.claim_token);
        return `已认领任务 ${payload.task.id}`;
      },
    );
  });

  dom.taskList.addEventListener("click", (event) => {
    const button = event.target.closest("button");
    if (!button) return;
    if (button.dataset.action === "retry") {
      loadTasks({ force: true }).catch(() => undefined);
    } else if (button.dataset.action === "empty-demo") {
      dom.createDemo.click();
    } else if (button.dataset.action === "start") {
      const taskId = Number(button.dataset.taskId);
      runAction(
        `start-${taskId}`,
        () => request(API.start(taskId), "POST", taskCredentials(taskId)),
        () => `任务 ${taskId} 已启动`,
      );
    } else if (button.dataset.action === "complete") {
      completeFiveTimes(button.dataset.taskId, button.dataset.sequence);
    }
  });

  renderSummary();
  renderLoading();
  loadTasks().catch(() => undefined);
  setInterval(() => loadTasks().catch(() => undefined), POLL_MS);
  setInterval(updateTimestamp, 1000);
})();
