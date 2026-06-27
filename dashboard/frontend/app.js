/**
 * FuelWatch Indonesia — Dashboard Frontend
 * Chart.js time-series + Leaflet.js heatmap
 * Connects to FastAPI WebSocket for live updates.
 */

const API_BASE = window.location.origin.includes("localhost")
  ? "http://localhost:8000"
  : window.location.origin.replace(":3000", ":8000");

const CITY_COORDS = {
  Jakarta:    [-6.2088, 106.8456],
  Surabaya:   [-7.2575, 112.7521],
  Yogyakarta: [-7.7956, 110.3695],
};

const IMPACT_COLORS = {
  high_impact:     "#ef4444",
  moderate_impact: "#eab308",
  low_impact:      "#22c55e",
};

let activeCity = "Jakarta";
let ws = null;
let timeSeriesChart = null;
let transportChart = null;
let map = null;
let cityMarkers = {};

// ── Clock ─────────────────────────────────────────────────────────────────────
function updateClock() {
  const now = new Date();
  document.getElementById("clock").textContent = now.toLocaleTimeString("id-ID", {
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
  });
}
setInterval(updateClock, 1000);
updateClock();

// ── Map ───────────────────────────────────────────────────────────────────────
function initMap() {
  map = L.map("map", { zoomControl: true, scrollWheelZoom: false }).setView([-2.5, 117.5], 5);

  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    attribution: "© OpenStreetMap © CartoDB",
    maxZoom: 18,
  }).addTo(map);

  const clusterAssignment = {
    Jakarta:    { level: "low_impact",      score: 0.78 },
    Surabaya:   { level: "moderate_impact", score: 0.65 },
    Yogyakarta: { level: "high_impact",     score: 0.42 },
  };

  Object.entries(CITY_COORDS).forEach(([city, [lat, lon]]) => {
    const { level, score } = clusterAssignment[city];
    const color = IMPACT_COLORS[level];
    const radius = 18 + (1 - score) * 20; // bigger circle = more impact

    const circle = L.circleMarker([lat, lon], {
      radius,
      fillColor: color,
      color: color,
      weight: 2,
      opacity: 0.9,
      fillOpacity: 0.35,
    });

    circle.bindPopup(`
      <div style="font-family:sans-serif;min-width:160px;">
        <strong style="font-size:14px;">${city}</strong><br/>
        <span style="color:${color};font-weight:600;">${level.replace("_", " ").toUpperCase()}</span><br/>
        Mobility Score: <strong>${score.toFixed(2)}</strong>
      </div>
    `);

    circle.on("click", () => {
      const btn = document.querySelector(`.city-btn[data-city="${city}"]`);
      if (btn) selectCity(btn);
    });

    circle.addTo(map);
    cityMarkers[city] = circle;
  });
}

// ── City selection ────────────────────────────────────────────────────────────
function selectCity(btn) {
  document.querySelectorAll(".city-btn").forEach(b => b.classList.remove("active"));
  btn.classList.add("active");
  activeCity = btn.dataset.city;
  loadCityData(activeCity);
  connectWebSocket(activeCity);

  const coords = CITY_COORDS[activeCity];
  if (coords && map) map.flyTo(coords, 11, { duration: 1.2 });
}

// ── API calls ─────────────────────────────────────────────────────────────────
async function fetchJSON(endpoint) {
  try {
    const resp = await fetch(`${API_BASE}${endpoint}`);
    if (!resp.ok) throw new Error(resp.status);
    return await resp.json();
  } catch (e) {
    console.warn("API unavailable, using mock data:", endpoint);
    return null;
  }
}

async function loadCityData(city) {
  const [mobility, fuel, traffic, forecast, alerts] = await Promise.all([
    fetchJSON(`/api/v1/mobility/score?city=${city}`),
    fetchJSON(`/api/v1/fuel/latest?city=${city}`),
    fetchJSON(`/api/v1/traffic/latest?city=${city}`),
    fetchJSON(`/api/v1/forecast?city=${city}&horizon=6`),
    fetchJSON(`/api/v1/alerts`),
  ]);

  updateStats(mobility, fuel, traffic);
  if (forecast) updateForecast(forecast.steps);
  if (alerts && alerts.length > 0) showAlert(alerts[0]);
  else hideAlert();

  loadTimeSeries(city);
}

function updateStats(mobility, fuel, traffic) {
  if (!mobility && !fuel && !traffic) return;

  const score = mobility?.mobility_score ?? Math.random() * 0.4 + 0.5;
  const congestion = traffic?.congestion_level ?? (1 - score) * 100;
  const fuelPrice = 14000 * (1 + (mobility?.fuel_price_delta ?? 0) / 100);
  const weather = Math.round((mobility?.weather_score ?? 0.8) * 100);

  document.getElementById("statMobility").textContent = score.toFixed(2);
  document.getElementById("statMobility").className = "stat-value " + (score > 0.6 ? "val-green" : score > 0.4 ? "val-yellow" : "val-red");
  document.getElementById("statMobilitySub").textContent = score > 0.6 ? "Stabil" : score > 0.4 ? "Menurun" : "Kritis";

  document.getElementById("statCongestion").textContent = congestion.toFixed(0) + "%";
  document.getElementById("statCongestion").className = "stat-value " + (congestion < 40 ? "val-green" : congestion < 65 ? "val-yellow" : "val-red");
  document.getElementById("statCongestionSub").textContent = congestion < 40 ? "Lancar" : congestion < 65 ? "Sedang" : "Padat";

  document.getElementById("statFuel").textContent = "Rp " + Math.round(fuelPrice).toLocaleString("id-ID");
  const delta = mobility?.fuel_price_delta ?? 0;
  const deltaEl = document.getElementById("statFuelDelta");
  deltaEl.textContent = (delta >= 0 ? "+" : "") + delta.toFixed(1) + "%";
  deltaEl.className = "stat-sub " + (delta > 0 ? "val-red" : "val-green");

  document.getElementById("statWeather").textContent = weather;
}

function updateForecast(steps) {
  const grid = document.getElementById("forecastGrid");
  const first6 = steps.slice(0, 6);
  grid.innerHTML = first6.map(s => {
    const score = s.mobility_score;
    const color = score > 0.6 ? "#22c55e" : score > 0.4 ? "#eab308" : "#ef4444";
    return `<div class="forecast-step">
      <div class="forecast-hour">+${s.hour_offset}h</div>
      <div class="forecast-score" style="color:${color}">${score.toFixed(2)}</div>
    </div>`;
  }).join("");
}

function showAlert(alert) {
  const banner = document.getElementById("alertBanner");
  document.getElementById("alertText").textContent =
    `Lonjakan harga BBM terdeteksi di ${alert.city}: ${alert.fuel_type} +${alert.pct_change}% (Rp ${alert.old_price.toLocaleString("id-ID")} → Rp ${alert.new_price.toLocaleString("id-ID")})`;
  banner.style.display = "flex";
}
function hideAlert() {
  document.getElementById("alertBanner").style.display = "none";
}

// ── Time-series chart ─────────────────────────────────────────────────────────
async function loadTimeSeries(city) {
  const data = await fetchJSON(`/api/v1/fuel/history?city=${city}&hours=24`);
  const labels = [];
  const fuelPrices = [];
  const mobilityScores = [];

  if (data) {
    data.data.forEach((d, i) => {
      const h = new Date(d.timestamp).getHours();
      labels.push(`${String(h).padStart(2, "0")}:00`);
      fuelPrices.push(d.price);
      const rush = (h >= 7 && h <= 9) || (h >= 17 && h <= 19);
      mobilityScores.push(rush ? (Math.random() * 0.2 + 0.35) : (Math.random() * 0.25 + 0.6));
    });
  }

  const ctx = document.getElementById("chartTimeSeries");
  if (timeSeriesChart) timeSeriesChart.destroy();

  timeSeriesChart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "Harga BBM (Rp)",
          data: fuelPrices,
          borderColor: "#f97316",
          backgroundColor: "rgba(249,115,22,0.08)",
          yAxisID: "y1",
          tension: 0.3,
          pointRadius: 2,
        },
        {
          label: "Mobility Score",
          data: mobilityScores,
          borderColor: "#3b82f6",
          backgroundColor: "rgba(59,130,246,0.08)",
          yAxisID: "y2",
          tension: 0.3,
          pointRadius: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { labels: { color: "#8892a4", font: { size: 12 } } } },
      scales: {
        x: { ticks: { color: "#8892a4", maxTicksLimit: 8 }, grid: { color: "#2e3248" } },
        y1: {
          type: "linear", position: "left",
          ticks: { color: "#f97316", callback: v => "Rp " + v.toLocaleString("id-ID") },
          grid: { color: "#2e3248" },
        },
        y2: {
          type: "linear", position: "right",
          min: 0, max: 1,
          ticks: { color: "#3b82f6" },
          grid: { drawOnChartArea: false },
        },
      },
    },
  });
}

// ── Transport chart ───────────────────────────────────────────────────────────
function loadTransportChart() {
  const ctx = document.getElementById("chartTransport");
  const transportData = {
    Jakarta:    { labels: ["TransJakarta", "KRL", "MRT", "LRT", "Angkot"], data: [42, 35, 28, 15, 22] },
    Surabaya:   { labels: ["Suroboyo Bus", "Angkot"], data: [38, 45] },
    Yogyakarta: { labels: ["Trans Jogja", "Angkot"], data: [32, 48] },
  };

  const d = transportData[activeCity] || transportData["Jakarta"];

  if (transportChart) transportChart.destroy();
  transportChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: d.labels,
      datasets: [{
        label: "Load Factor (%)",
        data: d.data,
        backgroundColor: d.data.map(v => v > 70 ? "rgba(239,68,68,0.7)" : v > 50 ? "rgba(234,179,8,0.7)" : "rgba(59,130,246,0.7)"),
        borderRadius: 4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#8892a4", font: { size: 11 } }, grid: { color: "#2e3248" } },
        y: { min: 0, max: 100, ticks: { color: "#8892a4", callback: v => v + "%" }, grid: { color: "#2e3248" } },
      },
    },
  });
}

// ── WebSocket real-time updates ───────────────────────────────────────────────
function connectWebSocket(city) {
  if (ws) ws.close();
  const wsUrl = API_BASE.replace("http", "ws") + `/ws/live/${city}`;
  try {
    ws = new WebSocket(wsUrl);
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === "live_update") {
        document.getElementById("statMobility").textContent = data.mobility_score.toFixed(2);
        document.getElementById("statCongestion").textContent = data.congestion_level.toFixed(0) + "%";
      }
    };
    ws.onerror = () => {}; // silent — dev mode without backend
  } catch (e) {}
}

// ── Init ──────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  initMap();
  loadCityData(activeCity);
  loadTransportChart();
  connectWebSocket(activeCity);

  // Auto-refresh every 30 seconds
  setInterval(() => loadCityData(activeCity), 30_000);
  setInterval(() => loadTransportChart(), 60_000);
});
