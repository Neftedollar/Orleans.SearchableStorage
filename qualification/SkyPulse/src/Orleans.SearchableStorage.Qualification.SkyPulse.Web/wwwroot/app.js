const form = document.querySelector("#query-form");
const rowsElement = document.querySelector("#rows");
const statusElement = document.querySelector("#status");
const streamState = document.querySelector("#stream-state");
const refreshButton = document.querySelector("#refresh");
const nextButton = document.querySelector("#next");
const capacityPanel = document.querySelector("#capacity-panel");
const capacityStatus = document.querySelector("#capacity-status");
const capacityTarget = document.querySelector("#capacity-target");
const capacityToken = document.querySelector("#capacity-token");
const capacityGrow = document.querySelector("#capacity-grow");

let sessionId = null;
let eventSource = null;
let refreshTimer = null;
let lastRequest = null;
let nextContinuationToken = null;

form.addEventListener("submit", async event => {
  event.preventDefault();
  lastRequest = buildRequest(null);
  await createSession(lastRequest);
});

refreshButton.addEventListener("click", () => refreshMembership());
nextButton.addEventListener("click", async () => {
  if (!nextContinuationToken) return;
  lastRequest = { ...lastRequest, continuationToken: nextContinuationToken };
  await createSession(lastRequest);
});

capacityGrow.addEventListener("click", requestCapacityGrowth);

function optionalMinimum(id) {
  const value = document.querySelector(id).value;
  return value === "" ? null : { minimum: Number(value) };
}

function buildRequest(continuationToken) {
  const activeSince = document.querySelector("#active-since").value;
  return {
    pageSize: Number(document.querySelector("#page-size").value),
    continuationToken,
    lastActivityMinuteUtc: activeSince
      ? { minimum: Math.floor(new Date(activeSince).getTime() / 60000) }
      : null,
    currentPostCount: optionalMinimum("#posts-min"),
    currentFollowerCount: optionalMinimum("#followers-min"),
    currentFollowingCount: optionalMinimum("#following-min"),
    postCreates1Day: optionalMinimum("#post-creates-min"),
    receivedEngagementCreates30Days: optionalMinimum("#engagement-min")
  };
}

async function createSession(request) {
  closeSession();
  setStatus("Running bounded query…");
  try {
    const response = await fetch("/api/query-sessions", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(request)
    });
    if (!response.ok) throw new Error(await response.text());
    applySnapshot(await response.json());
    connectEvents();
    refreshTimer = window.setInterval(refreshMembership, 15000);
  } catch (error) {
    setStatus(`Query failed: ${error.message}`);
  }
}

async function refreshMembership() {
  if (!sessionId) return;
  try {
    const response = await fetch(`/api/query-sessions/${sessionId}/refresh`, { method: "POST" });
    if (response.status === 404 || response.status === 409) {
      await createSession(lastRequest);
      return;
    }
    if (!response.ok) throw new Error(await response.text());
    applySnapshot(await response.json());
  } catch (error) {
    setStatus(`Membership refresh failed: ${error.message}`);
  }
}

function applySnapshot(snapshot) {
  sessionId = snapshot.sessionId;
  nextContinuationToken = snapshot.page.continuationToken;
  renderRows(snapshot.page.rows);
  refreshButton.disabled = false;
  nextButton.disabled = !nextContinuationToken;
  setStatus(`${snapshot.page.rows.length} grain IDs in the current bounded page.`);
}

function connectEvents() {
  eventSource = new EventSource(`/api/query-sessions/${sessionId}/events`);
  eventSource.addEventListener("open", () => setStreamState(true));
  eventSource.addEventListener("projection", event => updateRow(JSON.parse(event.data)));
  eventSource.addEventListener("resync", async () => {
    setStatus("Live membership changed. Re-running the bounded query.");
    eventSource.close();
    await createSession(lastRequest);
  });
  eventSource.onerror = () => setStreamState(false);
}

function closeSession() {
  if (eventSource) eventSource.close();
  if (refreshTimer) window.clearInterval(refreshTimer);
  if (sessionId) fetch(`/api/query-sessions/${sessionId}`, { method: "DELETE", keepalive: true });
  eventSource = null;
  refreshTimer = null;
  sessionId = null;
  setStreamState(false);
}

function renderRows(rows) {
  rowsElement.replaceChildren(...rows.map(createRow));
}

function updateRow(row) {
  const existing = document.getElementById(`grain-${row.grainId}`);
  if (!existing) return;
  const replacement = createRow(row);
  replacement.classList.add("changed");
  existing.replaceWith(replacement);
}

function createRow(row) {
  const tr = document.createElement("tr");
  tr.id = `grain-${row.grainId}`;
  const values = [
    row.grainId,
    formatMinute(row.lastActivityMinuteUtc),
    row.currentPostCount,
    row.currentFollowerCount,
    row.currentFollowingCount,
    row.postCreates1Day,
    row.receivedEngagementCreates30Days,
    `${row.createdRecordCount30Days} / ${row.updatedRecordCount30Days} / ${row.deletedRecordCount30Days}`
  ];
  for (const value of values) {
    const td = document.createElement("td");
    td.textContent = value;
    tr.appendChild(td);
  }
  return tr;
}

function formatMinute(unixMinute) {
  return new Date(unixMinute * 60000).toISOString().replace("T", " ").slice(0, 16) + " UTC";
}

function setStreamState(online) {
  streamState.textContent = online ? "Live page updates" : "Disconnected";
  streamState.className = `state ${online ? "state-online" : "state-offline"}`;
}

function setStatus(message) {
  statusElement.textContent = message;
}

async function refreshCapacity() {
  try {
    const response = await fetch("/api/corpus-capacity", { cache: "no-store" });
    if (response.status === 404) {
      capacityPanel.hidden = true;
      return;
    }

    capacityPanel.hidden = false;
    if (!response.ok) {
      capacityStatus.textContent = `Capacity state unavailable (${response.status}).`;
      capacityGrow.disabled = true;
      return;
    }

    renderCapacity(await response.json());
  } catch (error) {
    capacityPanel.hidden = false;
    capacityStatus.textContent = `Capacity state failed: ${error.message}`;
    capacityGrow.disabled = true;
  }
}

function renderCapacity(capacity) {
  const requested = capacity.requestedCorpusCap
    ? `; requested ${formatCount(capacity.requestedCorpusCap)}`
    : "";
  capacityStatus.textContent =
    `Phase ${capacity.phase}; active ${formatCount(capacity.activeCorpusCap)}${requested}; `
    + `admitted ${formatCount(capacity.admissionCorpusCap)}; PostgreSQL `
    + `${formatCount(capacity.postgreSqlAccountCount)}; synchronized `
    + `${formatCount(capacity.synchronizedAccountCount)}.`;

  const selected = capacityTarget.value;
  capacityTarget.replaceChildren(...capacity.availableTargets.map(target => {
    const option = document.createElement("option");
    option.value = target.profileId;
    option.textContent = `${target.profileId} — ${formatCount(target.corpusCap)}`;
    return option;
  }));
  if ([...capacityTarget.options].some(option => option.value === selected)) {
    capacityTarget.value = selected;
  }

  capacityGrow.disabled = capacity.availableTargets.length === 0;
}

async function requestCapacityGrowth() {
  const profileId = capacityTarget.value;
  const token = capacityToken.value;
  if (!profileId || !token) {
    capacityStatus.textContent = "Select a reviewed target and enter the administrative token.";
    return;
  }

  capacityGrow.disabled = true;
  try {
    const response = await fetch(`/api/corpus-capacity/${encodeURIComponent(profileId)}`, {
      method: "POST",
      headers: { "X-SkyPulse-Corpus-Admin": token }
    });
    capacityToken.value = "";
    if (!response.ok && response.status !== 202) throw new Error(await response.text());
    renderCapacity((await response.json()).capacity);
  } catch (error) {
    capacityToken.value = "";
    capacityStatus.textContent = `Capacity increase failed: ${error.message}`;
  } finally {
    await refreshCapacity();
  }
}

function formatCount(value) {
  return Number(value).toLocaleString("en-US");
}

refreshCapacity();
window.setInterval(refreshCapacity, 5000);

window.addEventListener("beforeunload", closeSession);
