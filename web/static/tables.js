/* Sort, row-expansion, and header tooltips for .fantasy-table elements.
   Called once on DOMContentLoaded and again after every HTMX swap. */

const ROW_STEPS = [25, 50, 100];

/* ── Column tooltip definitions ──────────────────────────────────────── */

const COL_TIPS = {
  // ── Shared / hitter ──────────────────────────────────────────────────
  "xwOBA": {
    label: "Expected Weighted On-Base Average",
    desc:  "Measures overall offensive value by weighting each outcome (single, HR, walk…) by its run value. 'Expected' means it's based on exit velocity and launch angle of contact, not actual results — so it strips out defensive luck and BABIP variance. Best single-number hitter quality metric.",
  },
  "xBA": {
    label: "Expected Batting Average",
    desc:  "What a hitter's batting average should be based on the quality of contact made, ignoring where fielders happened to be. High xBA with low BA = unlucky; low xBA with high BA = lucky.",
  },
  "xSLG": {
    label: "Expected Slugging Percentage",
    desc:  "Slugging % based on contact quality (exit velo + launch angle) rather than actual outcomes. Better than SLG at predicting future power production.",
  },
  "EV": {
    label: "Exit Velocity",
    desc:  "Average speed off the bat (mph) on all batted balls. Harder contact leads to more hits and more power. For pitchers, this shows opponent EV — Savant inverts the percentile so 99 = softest contact allowed.",
  },
  "Brl%": {
    label: "Barrel Rate",
    desc:  "% of batted balls classified as 'barrels' — a tight exit velocity + launch angle window that historically produces a ~1.500 SLG. The purest power contact metric. For pitchers: 99 = fewest barrels allowed.",
  },
  "HH%": {
    label: "Hard Hit Rate",
    desc:  "% of batted balls hit at 95+ mph. Measures how consistently a hitter makes hard contact. For pitchers: 99 = softest contact profile allowed.",
  },
  "BSpd": {
    label: "Bat Speed",
    desc:  "Average swing speed measured at the barrel (mph). Faster bat speed gives a hitter more time to read pitches and still make hard contact. Tracked via Hawk-Eye since 2024.",
  },
  "Sq%": {
    label: "Squared-Up Rate",
    desc:  "% of swings where the batter makes flush contact — bat center meets ball center. High Sq% with high bat speed is the ideal power combination.",
  },
  "Chs%": {
    label: "Chase Rate",
    desc:  "% of pitches outside the strike zone that the batter swings at. Lower = more disciplined plate approach. Percentile is inverted for hitters: 99 = least chasing in MLB.",
  },
  "Whf%": {
    label: "Whiff Rate",
    desc:  "% of all swings that miss completely. Lower = better contact skills for hitters. Percentile inverted: 99 = fewest whiffs. For pitchers: higher = more swing-and-miss stuff; 99 = most whiffs induced.",
  },
  "K%": {
    label: "Strikeout Rate",
    desc:  "% of plate appearances ending in strikeout. For hitters: lower is better; percentile inverted so 99 = lowest K rate. For pitchers: higher is better (more strikeouts); 99 = highest K rate.",
  },
  "BB%": {
    label: "Walk Rate",
    desc:  "% of plate appearances ending in a walk. For hitters: higher = more patient/disciplined; 99 = most walks drawn. For pitchers: lower = better control; percentile inverted so 99 = fewest walks issued.",
  },
  "Spd": {
    label: "Sprint Speed",
    desc:  "Peak running speed in ft/sec, measured on the fastest runs each player makes (home-to-first, outfield routes, etc.). 99 = fastest runner in MLB. Matters for stolen base projection and range.",
  },
  // ── Pitcher-only ─────────────────────────────────────────────────────
  "xERA": {
    label: "Expected ERA",
    desc:  "What a pitcher's ERA should be based on the quality of contact allowed — exit velocity, launch angle, strikeouts, and walks. Strips out defensive luck and BABIP variance. The best single-season ERA predictor. Lower raw stat = better; Savant inverts so 99 = best.",
  },
  "FBv": {
    label: "Fastball Velocity",
    desc:  "Average speed of all fastball-type pitches (4-seam, sinker, cutter). Higher velo correlates with swing-and-miss and fewer hard-hit balls. 99 = hardest thrower in MLB.",
  },
  // ── Scoring model cols ───────────────────────────────────────────────
  "composite_score": {
    label: "Composite Score",
    desc:  "80% projected fantasy pts/game + 20% quality signal (xwOBA z-score for hitters, xFIP z-score for pitchers). Used to rank players.",
  },
  "pts_per_game": {
    label: "Points Per Game",
    desc:  "Projected fantasy points per game started/appeared, based on expected rate stats (K%, BB%, xSLG, xOBP for hitters; xERA, K%, BB%, IP for pitchers) × league scoring weights.",
  },
  "pts_per_week": {
    label: "Points Per Week",
    desc:  "pts/game × expected appearances per week. For starters: based on rotation cadence. For relievers: based on historical appearances/week.",
  },
  "proj_week_pts": {
    label: "Projected Week Points",
    desc:  "pts/game × actual games your team plays this week (from MLB schedule). Best estimate of fantasy scoring for the coming week.",
  },
  "Delta": {
    label: "Score Delta",
    desc:  "FA composite score minus your current player's score. Positive = the free agent is better. Sorted highest-first so biggest upgrades are at the top.",
  },
  "xwOBA_zscore": {
    label: "xwOBA Z-Score",
    desc:  "How many standard deviations above/below league average this player's xwOBA is. Used as the 20% quality tiebreaker in composite score.",
  },
  "xFIP_zscore": {
    label: "xFIP Z-Score",
    desc:  "How many standard deviations above/below league average this pitcher's xFIP is (inverted — positive = better). Used as the 20% quality tiebreaker in composite score.",
  },
};

/* ── Tooltip init ─────────────────────────────────────────────────────── */

function _colName(th) {
  // Get text content of th excluding the sort-indicator span
  return Array.from(th.childNodes)
    .filter(n => n.nodeType === Node.TEXT_NODE)
    .map(n => n.textContent.trim())
    .join("")
    .trim();
}

function _addTooltips(table) {
  table.querySelectorAll("thead th").forEach(th => {
    const name = _colName(th);
    const tip = COL_TIPS[name];
    if (!tip) return;

    th.setAttribute("data-bs-toggle", "tooltip");
    th.setAttribute("data-bs-placement", "bottom");
    th.setAttribute("data-bs-html", "true");
    th.setAttribute("title",
      `<strong>${tip.label}</strong><br><span class="tip-desc">${tip.desc}</span>`
    );

    new bootstrap.Tooltip(th, {
      delay: { show: 700, hide: 100 },
      trigger: "hover",
    });
  });
}

/* ── Sort ─────────────────────────────────────────────────────────────── */

function _addSortHandlers(table) {
  table.querySelectorAll("thead th").forEach((th, idx) => {
    th.style.cursor = "pointer";
    th.dataset.sortDir = "";
    const ind = document.createElement("span");
    ind.className = "sort-ind";
    ind.textContent = " ↕";
    th.appendChild(ind);
    th.addEventListener("click", () => _sortBy(table, idx, th));
  });
}

function _sortBy(table, colIdx, th) {
  const tbody = table.querySelector("tbody");
  const asc = th.dataset.sortDir !== "asc";

  const rows = Array.from(tbody.querySelectorAll("tr"));
  rows.sort((a, b) => {
    const ac = a.cells[colIdx];
    const bc = b.cells[colIdx];
    const av = ac.dataset.val !== undefined ? ac.dataset.val : ac.textContent.trim();
    const bv = bc.dataset.val !== undefined ? bc.dataset.val : bc.textContent.trim();

    const an = parseFloat(av);
    const bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
    if (av === "—") return 1;
    if (bv === "—") return -1;
    return asc ? av.localeCompare(bv) : bv.localeCompare(av);
  });

  rows.forEach(r => tbody.appendChild(r));

  table.querySelectorAll("thead th").forEach(t => {
    t.dataset.sortDir = "";
    const i = t.querySelector(".sort-ind");
    if (i) i.textContent = " ↕";
  });
  th.dataset.sortDir = asc ? "asc" : "desc";
  const i = th.querySelector(".sort-ind");
  if (i) i.textContent = asc ? " ↑" : " ↓";
}

/* ── Row expansion ────────────────────────────────────────────────────── */

function _addRowControls(table) {
  const rows = Array.from(table.querySelectorAll("tbody tr"));
  const total = rows.length;
  if (total <= ROW_STEPS[0]) return;

  const steps = ROW_STEPS.filter(n => n < total);
  const buttons = steps.map(n =>
    `<button class="btn btn-xs row-btn me-1" data-limit="${n}">${n}</button>`
  ).join("");
  const allBtn = `<button class="btn btn-xs row-btn me-1" data-limit="${total}">All (${total})</button>`;

  const bar = document.createElement("div");
  bar.className = "row-controls";
  bar.innerHTML = `<span class="text-muted small me-2">Rows:</span>${buttons}${allBtn}`;
  table.parentElement.insertBefore(bar, table);

  _applyLimit(table, rows, ROW_STEPS[0]);
  bar.querySelector("[data-limit]").classList.add("active");

  bar.addEventListener("click", e => {
    const btn = e.target.closest("[data-limit]");
    if (!btn) return;
    bar.querySelectorAll("[data-limit]").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    _applyLimit(table, Array.from(table.querySelectorAll("tbody tr")),
                parseInt(btn.dataset.limit));
  });
}

function _applyLimit(table, rows, limit) {
  rows.forEach((r, i) => { r.style.display = i < limit ? "" : "none"; });
}

/* ── Bootstrap ────────────────────────────────────────────────────────── */

function initTable(table) {
  if (table.dataset.initialized) return;
  table.dataset.initialized = "1";

  _addSortHandlers(table);
  _addRowControls(table);
  _addTooltips(table);
}

function initAllTables(root) {
  (root || document).querySelectorAll(".fantasy-table").forEach(initTable);
}

document.addEventListener("DOMContentLoaded", () => initAllTables());
document.addEventListener("htmx:afterSettle", e => initAllTables(e.detail.elt));
