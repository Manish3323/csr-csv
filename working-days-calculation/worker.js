importScripts('https://cdn.sheetjs.com/xlsx-0.20.3/package/dist/xlsx.full.min.js');

// ─── Date utilities ──────────────────────────────────────────────────────────

/**
 * Parse Fault Browser date string: "17-12-2025 11:59:57 PM"
 * Returns a Date or null.
 */
function parseFBDate(val) {
  if (!val) return null;
  if (val instanceof Date) return isNaN(val.getTime()) ? null : val;

  const s = String(val).trim();

  // DD-MM-YYYY HH:MM:SS [AM|PM]
  const m = s.match(/^(\d{1,2})-(\d{1,2})-(\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})(?:\s+(AM|PM))?$/i);
  if (m) {
    let [, dd, mo, yyyy, h, min, sec, ampm] = m;
    h = parseInt(h, 10); min = parseInt(min, 10); sec = parseInt(sec, 10);
    if (ampm) {
      if (ampm.toUpperCase() === 'PM' && h !== 12) h += 12;
      if (ampm.toUpperCase() === 'AM' && h === 12) h = 0;
    }
    return new Date(parseInt(yyyy, 10), parseInt(mo, 10) - 1, parseInt(dd, 10), h, min, sec);
  }

  // ISO or other parseable
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}

/**
 * Convert any cell value (Date, Excel serial number, string) to a JS Date.
 */
function toDate(val) {
  if (!val && val !== 0) return null;
  if (val instanceof Date) return isNaN(val.getTime()) ? null : val;
  if (typeof val === 'number') {
    // Excel date serial (integer part = days since 1899-12-30)
    const epoch = new Date(1899, 11, 30);
    const d = new Date(epoch.getTime() + val * 86400000);
    return isNaN(d.getTime()) ? null : d;
  }
  return parseFBDate(String(val));
}

/**
 * Combine a date cell value + time cell value into one Date.
 * Both may be Date objects, numbers (Excel serial / time fraction), or strings.
 */
function combineDateTime(dateVal, timeVal) {
  const d = toDate(dateVal);
  if (!d) return null;
  if (timeVal == null) return d;

  if (timeVal instanceof Date && !isNaN(timeVal.getTime())) {
    d.setHours(timeVal.getHours(), timeVal.getMinutes(), timeVal.getSeconds());
  } else if (typeof timeVal === 'number') {
    // Fraction of a day
    const totalSec = Math.round(timeVal * 86400);
    d.setHours(Math.floor(totalSec / 3600), Math.floor((totalSec % 3600) / 60), totalSec % 60);
  } else if (typeof timeVal === 'string') {
    const t = timeVal.match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
    if (t) d.setHours(parseInt(t[1], 10), parseInt(t[2], 10), parseInt(t[3] || 0, 10));
  }
  return d;
}

/** Format Date to YYYY-MM-DD */
function fmtDate(d) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

// ─── Reference data builders ──────────────────────────────────────────────────

/**
 * Build public-holiday lookup.
 * Returns Map<"STATENAME|YYYY-MM-DD", holidayLabel>
 * Weekends are excluded here — handled by day-of-week check.
 */
function buildCalendarMap(ws) {
  const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
  const map = new Map();

  if (!rows.length) return map;
  const keys = Object.keys(rows[0]);

  const dateKey    = keys.find(k => /date/i.test(k));
  const stateKey   = keys.find(k => /state/i.test(k));
  const holidayKey = keys.find(k => /holiday/i.test(k));

  if (!dateKey || !stateKey || !holidayKey) {
    self.postMessage({ type: 'warn', msg: 'Calendar sheet: could not detect DATE / STATE / HOLIDAY columns' });
    return map;
  }

  for (const row of rows) {
    const label = String(row[holidayKey] || '').trim();
    if (!label || /weekend/i.test(label)) continue;   // skip weekend entries

    const d = toDate(row[dateKey]);
    if (!d) continue;

    const state   = String(row[stateKey] || '').trim().toUpperCase();
    const dateStr = fmtDate(d);
    map.set(`${state}|${dateStr}`, label);
  }

  return map;
}

/**
 * Build ATM master lookup from DB sheet.
 * Returns Map<terminalId, { state, lho }>
 */
function buildDbMap(wb) {
  const sheetName = wb.SheetNames.find(n => /^db$/i.test(n.trim())) || wb.SheetNames[0];
  const rows = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], { defval: '' });
  const map = new Map();

  if (!rows.length) return map;
  const keys = Object.keys(rows[0]);

  const idKey    = keys.find(k => /terminal\s*id/i.test(k));
  const stateKey = keys.find(k => /statename|^state$/i.test(k));
  const lhoKey   = keys.find(k => /circle|^lho$/i.test(k));

  if (!idKey) {
    self.postMessage({ type: 'warn', msg: 'DB sheet: could not find TERMINAL ID column' });
    return map;
  }

  for (const row of rows) {
    const id = String(row[idKey] || '').trim();
    if (!id) continue;
    map.set(id, {
      state : stateKey ? String(row[stateKey] || '').trim().toUpperCase() : '',
      lho   : lhoKey   ? String(row[lhoKey]   || '').trim() : '',
    });
  }

  return map;
}

// ─── Core downtime calculator ─────────────────────────────────────────────────

/**
 * Calculate downtime breakdown between two Date objects.
 * Banking hours: 09:00–18:00 on working (non-holiday) days.
 * Returns hours (2 dp) for each category.
 */
function calcDowntime(startDt, endDt, state, calMap) {
  const ZERO = { postBanking: 0, weekend: 0, publicHol: 0, actual: 0, total: 0 };
  if (!startDt || !endDt || endDt <= startDt) return ZERO;

  const normState = (state || '').toUpperCase();
  let postBanking = 0, weekend = 0, publicHol = 0, actual = 0;

  let cur = new Date(startDt);

  while (cur < endDt) {
    const y = cur.getFullYear(), mo = cur.getMonth(), day = cur.getDate();
    const dayStart = new Date(y, mo, day);
    const dayEnd   = new Date(y, mo, day + 1);

    const segStartMs = cur.getTime();
    const segEndMs   = Math.min(endDt.getTime(), dayEnd.getTime());
    const segHrs     = (segEndMs - segStartMs) / 3600000;

    const dow        = dayStart.getDay();              // 0=Sun,6=Sat
    const isWeekend  = (dow === 0 || dow === 6);
    const dateStr    = fmtDate(dayStart);
    const isPubHol   = !isWeekend && calMap.has(`${normState}|${dateStr}`);

    if (isWeekend) {
      weekend += segHrs;
    } else if (isPubHol) {
      publicHol += segHrs;
    } else {
      // Working day — split by banking window (09:00–18:00)
      const bankS = new Date(y, mo, day,  9, 0, 0).getTime();
      const bankE = new Date(y, mo, day, 18, 0, 0).getTime();

      const preMs  = Math.max(0, Math.min(bankS, segEndMs) - segStartMs);
      const bankMs = Math.max(0, Math.min(bankE, segEndMs) - Math.max(bankS, segStartMs));
      const postMs = Math.max(0, segEndMs - Math.max(bankE, segStartMs));

      postBanking += (preMs + postMs) / 3600000;
      actual      += bankMs / 3600000;
    }

    cur = new Date(dayEnd);
  }

  const r = n => Math.round(n * 100) / 100;
  return {
    postBanking : r(postBanking),
    weekend     : r(weekend),
    publicHol   : r(publicHol),
    actual      : r(actual),
    total       : r((endDt.getTime() - startDt.getTime()) / 3600000),
  };
}

// ─── Fault Browser file parser ────────────────────────────────────────────────

/**
 * Parse a Fault Browser Excel export.
 * Skips metadata rows at top (up to 10 rows), finds the real header row
 * by looking for "Terminal" in a cell, then returns array of row objects.
 */
function parseFaultBrowserFile(ab) {
  const wb = XLSX.read(ab, { type: 'array', cellDates: true });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const raw = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });

  // Look for the row where a cell is exactly "Terminal ID" (not a substring match,
  // which would wrongly match the metadata row containing "Terminals: OKIP13 ...").
  let headerIdx = 0;
  for (let i = 0; i < Math.min(10, raw.length); i++) {
    if (raw[i].some(c => String(c).trim() === 'Terminal ID')) {
      headerIdx = i;
      break;
    }
  }

  const headers = raw[headerIdx].map(h => String(h).trim());
  const rows = [];

  for (let i = headerIdx + 1; i < raw.length; i++) {
    const r = raw[i];
    if (r.every(c => c === '' || c == null)) continue;
    const obj = {};
    headers.forEach((h, j) => { obj[h] = r[j]; });
    rows.push(obj);
  }

  return rows;
}

// ─── Pipeline 1 ───────────────────────────────────────────────────────────────

function processPipeline1(files, dbAb, calAb) {
  progress(5, 'Parsing DB Master...');
  const dbWb  = XLSX.read(dbAb,  { type: 'array', cellDates: true });
  const dbMap = buildDbMap(dbWb);

  progress(10, 'Parsing Holiday Calendar...');
  const calWb      = XLSX.read(calAb, { type: 'array', cellDates: true });
  const calSheet   = calWb.SheetNames.find(n => /cal|holiday/i.test(n)) || calWb.SheetNames[0];
  const calMap     = buildCalendarMap(calWb.Sheets[calSheet]);

  // Parse and merge all Fault Browser files
  let allRows = [];
  for (let i = 0; i < files.length; i++) {
    progress(10 + Math.round((i / files.length) * 30),
      `Parsing file ${i + 1}/${files.length}: ${files[i].name}`);
    allRows = allRows.concat(parseFaultBrowserFile(files[i].data));
  }

  // Detect column names from first row
  const sample  = allRows[0] || {};
  const cols    = Object.keys(sample);
  const termCol = cols.find(c => /terminal.?id/i.test(c))                        || 'Terminal ID';
  const startCol= cols.find(c => /start|started/i.test(c))                       || 'Started at';
  const endCol  = cols.find(c => /end|ended/i.test(c) && !/start/i.test(c))      || 'Ended at';
  const faultCol= cols.find(c => /fault/i.test(c))                               || 'Fault';
  const durCol  = cols.find(c => /duration|minute/i.test(c))                     || 'Duration (minutes)';

  progress(42, `Sorting ${allRows.length.toLocaleString()} records by start time...`);
  allRows.sort((a, b) => {
    const da = parseFBDate(a[startCol]);
    const db = parseFBDate(b[startCol]);
    if (!da) return 1;
    if (!db) return -1;
    return da - db;
  });

  progress(50, 'Calculating downtime...');
  const results = [];
  const total   = allRows.length;
  let lastPct   = 50;

  for (let i = 0; i < total; i++) {
    const row    = allRows[i];
    const termId = String(row[termCol] || '').trim();
    const startDt= parseFBDate(row[startCol]);
    const endDt  = parseFBDate(row[endCol]);
    const dbInfo = dbMap.get(termId) || {};
    const state  = dbInfo.state || '';
    const calc   = calcDowntime(startDt, endDt, state, calMap);

    results.push({
      'Terminal ID'                       : termId,
      'State'                             : state,
      'LHO/Circle'                        : dbInfo.lho || '',
      'Fault'                             : row[faultCol] || '',
      'Started at'                        : row[startCol] || '',
      'Ended at'                          : row[endCol]   || '',
      'Duration (minutes)'                : row[durCol]   || '',
      'Post Banking Hours (00-09 & 18-24)': calc.postBanking,
      'Weekend Holiday Hours'             : calc.weekend,
      'Public Holiday Hours'              : calc.publicHol,
      'Actual Downtime (hrs)'             : calc.actual,
      'Total Downtime (hrs)'              : calc.total,
    });

    const newPct = 50 + Math.round((i / total) * 45);
    if (newPct > lastPct) {
      lastPct = newPct;
      progress(newPct, `Processing ${(i + 1).toLocaleString()} / ${total.toLocaleString()} records...`);
    }
  }

  return results;
}

// ─── Pipeline 2 ───────────────────────────────────────────────────────────────

function processPipeline2(fileAb, dbAb, calAb) {
  progress(5, 'Parsing DB Master...');
  const dbWb  = XLSX.read(dbAb,  { type: 'array', cellDates: true });
  const dbMap = buildDbMap(dbWb);

  progress(15, 'Parsing Holiday Calendar...');
  const calWb    = XLSX.read(calAb, { type: 'array', cellDates: true });
  const calSheet = calWb.SheetNames.find(n => /cal|holiday/i.test(n)) || calWb.SheetNames[0];
  const calMap   = buildCalendarMap(calWb.Sheets[calSheet]);

  progress(25, 'Parsing fault data file...');
  const faultWb  = XLSX.read(fileAb, { type: 'array', cellDates: true });

  // Prefer Sheet1 or Data sheet; fallback to first
  const sheetName = faultWb.SheetNames.find(n => /^(sheet1|data)$/i.test(n.trim())) || faultWb.SheetNames[0];
  const rows = XLSX.utils.sheet_to_json(faultWb.Sheets[sheetName], { defval: '' });

  if (!rows.length) throw new Error(`No data rows found in sheet "${sheetName}"`);

  // ─ Column detection ─────────────────────────────────────────────────────────
  const cols       = Object.keys(rows[0]);
  const idCol      = cols.find(c => /atm.?id|terminal.?id/i.test(c))            || cols[0];
  const stateCol   = cols.find(c => /^state(name)?$/i.test(c.trim()));
  const startDtCol = cols.find(c => /start.?date/i.test(c));
  const startTmCol = cols.find(c => /start.?time/i.test(c));
  const endDtCol   = cols.find(c => /end.?date/i.test(c));
  const endTmCol   = cols.find(c => /end.?time/i.test(c));
  // Fallback: single combined datetime column
  const startFbCol = !startDtCol ? cols.find(c => /started?/i.test(c)) : null;
  const endFbCol   = !endDtCol   ? cols.find(c => /ended?/i.test(c) && !/start/i.test(c)) : null;

  progress(35, `Found ${rows.length.toLocaleString()} records, calculating downtime...`);

  const results = [];
  const total   = rows.length;
  let lastPct   = 35;

  for (let i = 0; i < total; i++) {
    const row    = rows[i];
    const termId = String(row[idCol] || '').trim();

    // Resolve state: prefer column in file, fallback to DB lookup
    let state = stateCol ? String(row[stateCol] || '').trim().toUpperCase() : '';
    if (!state) {
      const dbInfo = dbMap.get(termId);
      if (dbInfo) state = dbInfo.state;
    }

    // Parse start/end datetimes
    let startDt, endDt;
    if (startDtCol) {
      startDt = combineDateTime(row[startDtCol], startTmCol ? row[startTmCol] : null);
    } else if (startFbCol) {
      startDt = parseFBDate(row[startFbCol]);
    }

    if (endDtCol) {
      endDt = combineDateTime(row[endDtCol], endTmCol ? row[endTmCol] : null);
    } else if (endFbCol) {
      endDt = parseFBDate(row[endFbCol]);
    }

    const calc = calcDowntime(startDt, endDt, state, calMap);

    // Output: all original columns + 5 calculated
    const outRow = {};
    cols.forEach(c => { outRow[c] = row[c]; });
    outRow['Post Banking Hours (00-09 & 18-24)'] = calc.postBanking;
    outRow['Weekend Holiday Hours']              = calc.weekend;
    outRow['Public Holiday Hours']               = calc.publicHol;
    outRow['Actual Downtime (hrs)']              = calc.actual;
    outRow['Total Downtime (hrs)']               = calc.total;

    results.push(outRow);

    const newPct = 35 + Math.round((i / total) * 60);
    if (newPct > lastPct) {
      lastPct = newPct;
      progress(newPct, `Processing ${(i + 1).toLocaleString()} / ${total.toLocaleString()} records...`);
    }
  }

  return results;
}

// ─── CSV serialiser ───────────────────────────────────────────────────────────

function toCSV(rows) {
  if (!rows.length) return '';
  const cols  = Object.keys(rows[0]);
  const esc   = v => {
    const s = v == null ? '' : String(v);
    return (s.includes(',') || s.includes('"') || s.includes('\n'))
      ? `"${s.replace(/"/g, '""')}"`
      : s;
  };
  const lines = [cols.map(esc).join(',')];
  for (const row of rows) lines.push(cols.map(c => esc(row[c])).join(','));
  return lines.join('\r\n');
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function progress(pct, msg) {
  self.postMessage({ type: 'progress', pct, msg });
}

// ─── Message handler ──────────────────────────────────────────────────────────

self.onmessage = function (e) {
  const msg = e.data;
  try {
    let results;
    if (msg.type === 'pipeline1') {
      results = processPipeline1(msg.files, msg.dbMaster, msg.calendar);
    } else if (msg.type === 'pipeline2') {
      results = processPipeline2(msg.file, msg.dbMaster, msg.calendar);
    } else {
      throw new Error('Unknown pipeline type: ' + msg.type);
    }

    progress(98, 'Generating CSV output...');
    const csv     = toCSV(results);
    const preview = results.slice(0, 20);
    self.postMessage({ type: 'done', csv, preview, total: results.length });
  } catch (err) {
    self.postMessage({ type: 'error', msg: err.message || String(err) });
  }
};
