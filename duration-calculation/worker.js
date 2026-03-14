importScripts('https://cdn.sheetjs.com/xlsx-0.20.3/package/dist/xlsx.full.min.js');

// ─── Date utilities ───────────────────────────────────────────────────────────

function toDate(val) {
  if (!val && val !== 0) return null;
  if (val instanceof Date) return isNaN(val.getTime()) ? null : val;
  if (typeof val === 'number') {
    const epoch = new Date(1899, 11, 30);
    const d = new Date(epoch.getTime() + val * 86400000);
    return isNaN(d.getTime()) ? null : d;
  }
  const d = new Date(String(val));
  return isNaN(d.getTime()) ? null : d;
}

/**
 * Combine a date cell + time cell (both as SheetJS Date objects) into one Date.
 * dateVal : Date at 00:00:00 (date-only cell)
 * timeVal : Date based at 1899-12-30 (time-only cell) OR a time-fraction number
 */
function combineDateTime(dateVal, timeVal) {
  const d = toDate(dateVal);
  if (!d) return null;
  if (!timeVal && timeVal !== 0) return d;

  if (timeVal instanceof Date && !isNaN(timeVal.getTime())) {
    d.setHours(timeVal.getHours(), timeVal.getMinutes(), timeVal.getSeconds(), 0);
  } else if (typeof timeVal === 'number') {
    const totalSec = Math.round(timeVal * 86400);
    d.setHours(Math.floor(totalSec / 3600), Math.floor((totalSec % 3600) / 60), totalSec % 60, 0);
  }
  return d;
}

function fmtDate(d) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

/**
 * Convert a SheetJS cell Date back to a readable string.
 * Time-only (year ≤ 1900) → "HH:MM:SS AM/PM"
 * Date-only (H/M/S = 0)   → "DD-MM-YYYY"
 * Full datetime            → "DD-MM-YYYY HH:MM:SS AM/PM"
 */
function cellDateToStr(d) {
  if (!(d instanceof Date) || isNaN(d.getTime())) return d == null ? '' : String(d);
  const dd   = String(d.getDate()).padStart(2, '0');
  const mo   = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  let   h    = d.getHours();
  const min  = String(d.getMinutes()).padStart(2, '0');
  const sec  = String(d.getSeconds()).padStart(2, '0');

  if (yyyy <= 1900) {
    // Time-only cell
    const ampm = h >= 12 ? 'PM' : 'AM';
    h = h % 12 || 12;
    return `${String(h).padStart(2, '0')}:${min}:${sec} ${ampm}`;
  }
  if (h === 0 && min === '00' && sec === '00') {
    return `${dd}-${mo}-${yyyy}`;
  }
  const ampm = h >= 12 ? 'PM' : 'AM';
  h = h % 12 || 12;
  return `${dd}-${mo}-${yyyy} ${String(h).padStart(2, '0')}:${min}:${sec} ${ampm}`;
}

/** Format total hours as [H]:MM (like Excel's [h]:mm) */
function fmtDuration(totalHours) {
  if (totalHours < 0) totalHours = 0;
  const h   = Math.floor(totalHours);
  const min = Math.round((totalHours - h) * 60);
  // handle rounding edge case
  if (min === 60) return `${h + 1}:00`;
  return `${h}:${String(min).padStart(2, '0')}`;
}

// ─── Calendar builder ─────────────────────────────────────────────────────────

/**
 * Build holiday lookup from the Calender sheet.
 * Returns:
 *   weekendDates : Set<"YYYY-MM-DD">           — Weekend Holiday dates (any state)
 *   pubHolMap    : Map<"STATE|YYYY-MM-DD", label>  — state-specific public holidays
 */
function buildCalendarMap(ws) {
  const rows = XLSX.utils.sheet_to_json(ws, { defval: '', cellDates: true });
  const weekendDates = new Set();
  const pubHolMap    = new Map();

  if (!rows.length) return { weekendDates, pubHolMap };
  const keys = Object.keys(rows[0]);
  const dateKey    = keys.find(k => /^date$/i.test(k.trim()));
  const stateKey   = keys.find(k => /^state$/i.test(k.trim()));
  const holidayKey = keys.find(k => /^holiday$/i.test(k.trim()));

  if (!dateKey || !stateKey || !holidayKey) {
    self.postMessage({ type: 'warn', msg: 'Calender sheet: could not find DATE / STATE / HOLIDAY columns' });
    return { weekendDates, pubHolMap };
  }

  for (const row of rows) {
    const label  = String(row[holidayKey] || '').trim();
    if (!label) continue;
    const d = toDate(row[dateKey]);
    if (!d) continue;
    const dateStr = fmtDate(d);
    const state   = String(row[stateKey] || '').trim().toUpperCase();

    if (/weekend/i.test(label)) {
      weekendDates.add(dateStr);
    } else {
      pubHolMap.set(`${state}|${dateStr}`, label);
    }
  }

  return { weekendDates, pubHolMap };
}

// ─── Banking hours parser ─────────────────────────────────────────────────────

/**
 * Parse banking start/end hours from the "Post banking Hours" column header.
 * Expected format: "Post banking Hours(00 to 09 & 18 to 24)"
 *   → off-banking = 00–09 and 18–24  →  banking = 09–18
 * Returns { bankStart: 9, bankEnd: 18 } (integer hours, 0–24).
 * Falls back to { bankStart: 9, bankEnd: 18 } if parsing fails.
 */
function parseBankingHours(colName) {
  const DEFAULT = { bankStart: 9, bankEnd: 18 };
  if (!colName) return DEFAULT;
  // Match pattern like "(00 to 09 & 18 to 24)"
  const m = String(colName).match(/\(\s*(\d+)\s+to\s+(\d+)\s*&\s*(\d+)\s+to\s+(\d+)\s*\)/i);
  if (!m) return DEFAULT;
  const [, , preEnd, postStart] = m.map(Number);
  // pre-banking  = 0–preEnd      (e.g. 0–9)
  // post-banking = postStart–24  (e.g. 18–24)
  // banking      = preEnd–postStart (e.g. 9–18)
  if (preEnd >= postStart || preEnd < 0 || postStart > 24) return DEFAULT;
  return { bankStart: preEnd, bankEnd: postStart };
}

// ─── Core downtime calculator ─────────────────────────────────────────────────

/**
 * Calculate downtime breakdown for [startDt, endDt).
 * Returns { postBanking, weekend, publicHol, actual, total } in hours.
 * Banking hours: bankStart–bankEnd on working (non-holiday, non-weekend) days.
 */
function calcDowntime(startDt, endDt, state, weekendDates, pubHolMap, bankStart, bankEnd) {
  const ZERO = { postBanking: 0, weekend: 0, publicHol: 0, actual: 0, total: 0 };
  if (!startDt || !endDt || endDt <= startDt) return ZERO;

  const normState  = (state || '').trim().toUpperCase();
  let postBanking = 0, weekend = 0, publicHol = 0, actual = 0;

  let cur = new Date(startDt);
  while (cur < endDt) {
    const y = cur.getFullYear(), mo = cur.getMonth(), day = cur.getDate();
    const dayEnd      = new Date(y, mo, day + 1);
    const segStartMs  = cur.getTime();
    const segEndMs    = Math.min(endDt.getTime(), dayEnd.getTime());
    const segHrs      = (segEndMs - segStartMs) / 3_600_000;

    const dow         = new Date(y, mo, day).getDay();   // 0=Sun, 6=Sat
    const dateStr     = `${y}-${String(mo+1).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
    const isWeekend   = (dow === 0 || dow === 6) || weekendDates.has(dateStr);
    const isPubHol    = !isWeekend && pubHolMap.has(`${normState}|${dateStr}`);

    if (isWeekend) {
      weekend += segHrs;
    } else if (isPubHol) {
      publicHol += segHrs;
    } else {
      const bankS = new Date(y, mo, day, bankStart, 0, 0).getTime();
      const bankE = new Date(y, mo, day, bankEnd,   0, 0).getTime();
      const preMs  = Math.max(0, Math.min(bankS, segEndMs) - segStartMs);
      const bankMs = Math.max(0, Math.min(bankE, segEndMs) - Math.max(bankS, segStartMs));
      const postMs = Math.max(0, segEndMs - Math.max(bankE, segStartMs));
      postBanking += (preMs + postMs) / 3_600_000;
      actual      += bankMs / 3_600_000;
    }
    cur = new Date(dayEnd);
  }

  const r = v => Math.round(v * 10000) / 10000;
  return {
    postBanking : r(postBanking),
    weekend     : r(weekend),
    publicHol   : r(publicHol),
    actual      : r(actual),
    total       : r((endDt.getTime() - startDt.getTime()) / 3_600_000),
  };
}

// ─── CSV serialiser ───────────────────────────────────────────────────────────

function toCSV(rows) {
  if (!rows.length) return '';
  const cols = Object.keys(rows[0]);
  const esc  = v => {
    const s = v == null ? '' : String(v);
    return (s.includes(',') || s.includes('"') || s.includes('\n'))
      ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [cols.map(esc).join(',')];
  for (const row of rows) lines.push(cols.map(c => esc(row[c])).join(','));
  return lines.join('\r\n');
}

function progress(pct, msg) {
  self.postMessage({ type: 'progress', pct, msg });
}

// ─── Main processor ───────────────────────────────────────────────────────────

function processDuration(fileAb) {
  progress(5, 'Reading workbook…');
  const wb = XLSX.read(fileAb, { type: 'array', cellDates: true });

  // ── Locate sheets ──────────────────────────────────────────────────────────
  const dataSheetName = wb.SheetNames.find(n => /^data$/i.test(n.trim())) || wb.SheetNames[0];
  const calSheetName  = wb.SheetNames.find(n => /cal/i.test(n.trim()));

  if (!calSheetName) throw new Error('Could not find a "Calender" sheet in the uploaded file.');

  progress(10, 'Building holiday calendar…');
  const { weekendDates, pubHolMap } = buildCalendarMap(wb.Sheets[calSheetName]);

  progress(15, 'Parsing data sheet…');
  const ws     = wb.Sheets[dataSheetName];
  // Read as raw arrays to handle two-row header (row1 = group labels, row2 = column names)
  const raw    = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null, cellDates: true });

  if (raw.length < 2) throw new Error('Data sheet has too few rows.');

  // Row index 0: group headers ("Mandatory Columns", "Optional", "Automation Output")
  // Row index 1: actual column names
  // Row index 2+: data
  const headerRow = raw[1];
  const colNames  = headerRow.map(v => (v == null ? '' : String(v).trim()));

  // Column index detection
  const idxOf = (...patterns) => {
    for (const pat of patterns) {
      const i = colNames.findIndex(c => pat.test(c));
      if (i !== -1) return i;
    }
    return -1;
  };

  const iATM      = idxOf(/atm.?id/i, /terminal.?id/i);
  const iState    = idxOf(/^state$/i);
  const iStartDt  = idxOf(/start.?date/i);
  const iStartTm  = idxOf(/start.?time/i);
  const iEndDt    = idxOf(/end.?date/i);
  const iEndTm    = idxOf(/end.?time/i);
  const iDuration = idxOf(/^duration$/i);
  const iPostBank = idxOf(/post.?bank/i);
  const iWeekend  = idxOf(/weekend/i);
  const iPubHol   = idxOf(/public.?hol/i);
  const iActual   = idxOf(/actual/i);
  const iTotal    = idxOf(/total/i);

  if (iATM < 0 || iStartDt < 0 || iStartTm < 0 || iEndDt < 0 || iEndTm < 0) {
    throw new Error(
      `Could not detect required columns. Found: ${colNames.filter(Boolean).join(', ')}`
    );
  }

  // ── Parse banking hours from column header ────────────────────────────────
  const postBankColName = iPostBank >= 0 ? colNames[iPostBank] : '';
  const { bankStart, bankEnd } = parseBankingHours(postBankColName);
  self.postMessage({ type: 'info', bankStart, bankEnd });

  const dataRows = raw.slice(2);   // skip both header rows
  const total    = dataRows.length;
  progress(20, `Found ${total.toLocaleString()} data rows. Banking hours: ${bankStart}:00–${bankEnd}:00. Processing…`);

  // ── Overlap tracking per ATM ──────────────────────────────────────────────
  // chain[atmId] = { chainStart: Date, chainEnd: Date }
  const chain = new Map();

  const results = [];
  let lastPct   = 20;
  let overlapCt = 0;

  for (let i = 0; i < total; i++) {
    const row = dataRows[i];

    // Stringify all original values
    const out = {};
    colNames.forEach((name, j) => {
      if (!name) return;
      const v = row[j];
      out[name] = (v instanceof Date) ? cellDateToStr(v) : (v == null ? '' : String(v));
    });

    const atmId = row[iATM] ? String(row[iATM]).trim() : null;

    // Parse start / end
    const startDt = combineDateTime(row[iStartDt], row[iStartTm]);
    const endDt   = combineDateTime(row[iEndDt],   row[iEndTm]);

    if (!startDt || !endDt || endDt < startDt) {
      // Keep row as-is, calculated columns blank
      if (iDuration >= 0 && colNames[iDuration]) out[colNames[iDuration]] = '';
      if (iPostBank >= 0 && colNames[iPostBank]) out[colNames[iPostBank]] = '';
      if (iWeekend  >= 0 && colNames[iWeekend])  out[colNames[iWeekend]]  = '';
      if (iPubHol   >= 0 && colNames[iPubHol])   out[colNames[iPubHol]]   = '';
      if (iActual   >= 0 && colNames[iActual])   out[colNames[iActual]]   = '';
      if (iTotal    >= 0 && colNames[iTotal])    out[colNames[iTotal]]    = '';
      results.push(out);
      continue;
    }

    // ── Overlap / chain logic ───────────────────────────────────────────────
    let effectiveStart = startDt;

    if (atmId) {
      const c = chain.get(atmId);
      if (c && startDt < c.chainEnd) {
        // This ticket starts before the previous chain ended → overlap
        // effective_duration = (chainEnd - chainStart) + (endDt - chainEnd)
        //                    = endDt - chainStart
        effectiveStart = c.chainStart;
        overlapCt++;
        // Extend chain end if this ticket ends later
        chain.set(atmId, { chainStart: c.chainStart, chainEnd: endDt > c.chainEnd ? endDt : c.chainEnd });
      } else {
        // No overlap — start a new chain
        chain.set(atmId, { chainStart: startDt, chainEnd: endDt });
      }
    }

    const state = iState >= 0 ? String(row[iState] || '').trim() : '';
    const calc  = calcDowntime(effectiveStart, endDt, state, weekendDates, pubHolMap, bankStart, bankEnd);

    // Duration as [H]:MM
    const durHrs = (endDt.getTime() - effectiveStart.getTime()) / 3_600_000;

    if (iDuration >= 0 && colNames[iDuration]) out[colNames[iDuration]] = fmtDuration(durHrs);
    if (iPostBank >= 0 && colNames[iPostBank]) out[colNames[iPostBank]] = calc.postBanking;
    if (iWeekend  >= 0 && colNames[iWeekend])  out[colNames[iWeekend]]  = calc.weekend;
    if (iPubHol   >= 0 && colNames[iPubHol])   out[colNames[iPubHol]]   = calc.publicHol;
    if (iActual   >= 0 && colNames[iActual])   out[colNames[iActual]]   = calc.actual;
    if (iTotal    >= 0 && colNames[iTotal])    out[colNames[iTotal]]    = calc.total;

    results.push(out);

    const newPct = 20 + Math.round((i / total) * 75);
    if (newPct > lastPct) {
      lastPct = newPct;
      progress(newPct, `Processing ${(i + 1).toLocaleString()} / ${total.toLocaleString()} rows…  (overlaps: ${overlapCt})`);
    }
  }

  progress(96, 'Generating CSV…');
  const csv     = toCSV(results);
  const preview = results.slice(0, 20);
  self.postMessage({ type: 'done', csv, preview, total: results.length, overlaps: overlapCt });
}

// ─── Message handler ──────────────────────────────────────────────────────────

self.onmessage = function (e) {
  try {
    processDuration(e.data.file);
  } catch (err) {
    self.postMessage({ type: 'error', msg: err.message || String(err) });
  }
};
