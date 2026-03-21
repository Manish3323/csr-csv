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

/** For preview table only — converts a Date cell value to readable string. */
function cellDateToStr(d) {
  if (!(d instanceof Date) || isNaN(d.getTime())) return d == null ? '' : String(d);
  const dd   = String(d.getDate()).padStart(2, '0');
  const mo   = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  let   h    = d.getHours();
  const min  = String(d.getMinutes()).padStart(2, '0');
  const sec  = String(d.getSeconds()).padStart(2, '0');
  if (yyyy <= 1900) {
    const ampm = h >= 12 ? 'PM' : 'AM';
    h = h % 12 || 12;
    return `${String(h).padStart(2, '0')}:${min}:${sec} ${ampm}`;
  }
  if (h === 0 && min === '00' && sec === '00') return `${dd}-${mo}-${yyyy}`;
  const ampm = h >= 12 ? 'PM' : 'AM';
  h = h % 12 || 12;
  return `${dd}-${mo}-${yyyy} ${String(h).padStart(2, '0')}:${min}:${sec} ${ampm}`;
}

/** Preview display only — hours as H.MM string. */
function fmtDuration(totalHours) {
  if (totalHours < 0) totalHours = 0;
  const h   = Math.floor(totalHours);
  const min = Math.round((totalHours - h) * 60);
  if (min === 60) return `${h + 1}.00`;
  return `${h}.${String(min).padStart(2, '0')}`;
}

// ─── Calendar builder ─────────────────────────────────────────────────────────

function buildCalendarMap(ws) {
  const rows = XLSX.utils.sheet_to_json(ws, { defval: '', cellDates: true });
  const weekendDates = new Set();
  const pubHolMap    = new Map();
  if (!rows.length) return { weekendDates, pubHolMap };
  const keys       = Object.keys(rows[0]);
  const dateKey    = keys.find(k => /^date$/i.test(k.trim()));
  const stateKey   = keys.find(k => /^state$/i.test(k.trim()));
  const holidayKey = keys.find(k => /^holiday$/i.test(k.trim()));
  if (!dateKey || !stateKey || !holidayKey) {
    self.postMessage({ type: 'warn', msg: 'Calender sheet: could not find DATE / STATE / HOLIDAY columns' });
    return { weekendDates, pubHolMap };
  }
  for (const row of rows) {
    const label = String(row[holidayKey] || '').trim();
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

function parseBankingHours(colName) {
  const DEFAULT = { bankStart: 9, bankEnd: 18 };
  if (!colName) return DEFAULT;
  const m = String(colName).match(/\(\s*(\d+)\s+to\s+(\d+)\s*&\s*(\d+)\s+to\s+(\d+)\s*\)/i);
  if (!m) return DEFAULT;
  const [, , preEnd, postStart] = m.map(Number);
  if (preEnd >= postStart || preEnd < 0 || postStart > 24) return DEFAULT;
  return { bankStart: preEnd, bankEnd: postStart };
}

// ─── Core downtime calculator ─────────────────────────────────────────────────

function calcDowntime(startDt, endDt, state, weekendDates, pubHolMap, bankStart, bankEnd) {
  const ZERO = { postBanking: 0, weekend: 0, publicHol: 0, actual: 0, total: 0 };
  if (!startDt || !endDt || endDt <= startDt) return ZERO;
  const normState = (state || '').trim().toUpperCase();
  let postBanking = 0, weekend = 0, publicHol = 0, actual = 0;
  let cur = new Date(startDt);
  while (cur < endDt) {
    const y = cur.getFullYear(), mo = cur.getMonth(), day = cur.getDate();
    const dayEnd     = new Date(y, mo, day + 1);
    const segStartMs = cur.getTime();
    const segEndMs   = Math.min(endDt.getTime(), dayEnd.getTime());
    const segHrs     = (segEndMs - segStartMs) / 3_600_000;
    const dow        = new Date(y, mo, day).getDay();
    const dateStr    = `${y}-${String(mo+1).padStart(2,'0')}-${String(day).padStart(2,'0')}`;
    const isWeekend  = (dow === 0 || dow === 6) || weekendDates.has(dateStr);
    const isPubHol   = !isWeekend && pubHolMap.has(`${normState}|${dateStr}`);
    if (isWeekend) {
      weekend += segHrs;
    } else if (isPubHol) {
      publicHol += segHrs;
    } else {
      const bankS  = new Date(y, mo, day, bankStart, 0, 0).getTime();
      const bankE  = new Date(y, mo, day, bankEnd,   0, 0).getTime();
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

function progress(pct, msg) {
  self.postMessage({ type: 'progress', pct, msg });
}

// ─── Main processor ───────────────────────────────────────────────────────────

function processDuration(fileAb) {
  progress(5, 'Reading workbook…');
  const wb = XLSX.read(fileAb, { type: 'array', cellDates: true });

  const dataSheetName = wb.SheetNames.find(n => /^data$/i.test(n.trim())) || wb.SheetNames[0];
  const calSheetName  = wb.SheetNames.find(n => /cal/i.test(n.trim()));
  if (!calSheetName) throw new Error('Could not find a "Calender" sheet in the uploaded file.');

  progress(10, 'Building holiday calendar…');
  const { weekendDates, pubHolMap } = buildCalendarMap(wb.Sheets[calSheetName]);

  progress(15, 'Parsing data sheet…');
  const ws  = wb.Sheets[dataSheetName];
  const raw = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null, cellDates: true });
  if (raw.length < 2) throw new Error('Data sheet has too few rows.');

  const colNames = raw[1].map(v => (v == null ? '' : String(v).trim()));

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
    throw new Error(`Could not detect required columns. Found: ${colNames.filter(Boolean).join(', ')}`);
  }

  const postBankColName = iPostBank >= 0 ? colNames[iPostBank] : '';
  const { bankStart, bankEnd } = parseBankingHours(postBankColName);
  self.postMessage({ type: 'info', bankStart, bankEnd });

  // Duration column indices — all will receive [h]:mm formatted cells
  const durCols = [iDuration, iPostBank, iWeekend, iPubHol, iActual, iTotal].filter(i => i >= 0);

  // Ensure worksheet !ref covers all duration columns (M–Q may be empty in source)
  const wsRef = XLSX.utils.decode_range(ws['!ref'] || 'A1');
  wsRef.e.c = Math.max(wsRef.e.c, ...durCols);
  ws['!ref'] = XLSX.utils.encode_range(wsRef);

  const dataRows = raw.slice(2);
  const total    = dataRows.length;
  progress(20, `Found ${total.toLocaleString()} data rows. Banking hours: ${bankStart}:00–${bankEnd}:00. Processing…`);

  const chain   = new Map();
  const preview = [];
  let lastPct   = 20;
  let overlapCt = 0;

  for (let i = 0; i < total; i++) {
    const row = dataRows[i];
    const r   = i + 2;  // worksheet row index (0-based; 2 header rows)

    // Helper: write a duration value directly into the worksheet cell with [h]:mm format
    // val is in hours; Excel stores durations as fraction of a day (hours/24)
    const setDurCell = (col, hrs) => {
      if (col < 0) return;
      ws[XLSX.utils.encode_cell({ r, c: col })] = { v: hrs / 24, t: 'n', z: '[h]:mm' };
    };

    const atmId   = row[iATM] ? String(row[iATM]).trim() : null;
    const startDt = combineDateTime(row[iStartDt], row[iStartTm]);
    const endDt   = combineDateTime(row[iEndDt],   row[iEndTm]);

    if (!startDt || !endDt || endDt < startDt) {
      for (const c of durCols) setDurCell(c, 0);
      if (preview.length < 20) {
        const obj = {};
        colNames.forEach((name, j) => {
          if (!name) return;
          const v = row[j];
          obj[name] = v instanceof Date ? cellDateToStr(v) : (v == null ? '' : String(v));
        });
        durCols.forEach(c => { if (colNames[c]) obj[colNames[c]] = '0.00'; });
        preview.push(obj);
      }
      continue;
    }

    // ── Overlap / chain logic ─────────────────────────────────────────────────
    let effectiveStart = startDt;
    if (atmId) {
      const c = chain.get(atmId);
      if (c && startDt < c.chainEnd) {
        effectiveStart = c.chainStart;
        overlapCt++;
        chain.set(atmId, { chainStart: c.chainStart, chainEnd: endDt > c.chainEnd ? endDt : c.chainEnd });
      } else {
        chain.set(atmId, { chainStart: startDt, chainEnd: endDt });
      }
    }

    const state  = iState >= 0 ? String(row[iState] || '').trim() : '';
    const calc   = calcDowntime(effectiveStart, endDt, state, weekendDates, pubHolMap, bankStart, bankEnd);
    const durHrs = (endDt.getTime() - effectiveStart.getTime()) / 3_600_000;

    // Write [h]:mm cells directly into the worksheet
    setDurCell(iDuration, durHrs);
    setDurCell(iPostBank, calc.postBanking);
    setDurCell(iWeekend,  calc.weekend);
    setDurCell(iPubHol,   calc.publicHol);
    setDurCell(iActual,   calc.actual);
    setDurCell(iTotal,    calc.total);

    // Preview (text representation for on-screen table)
    if (preview.length < 20) {
      const obj = {};
      colNames.forEach((name, j) => {
        if (!name) return;
        const v = row[j];
        obj[name] = v instanceof Date ? cellDateToStr(v) : (v == null ? '' : String(v));
      });
      if (iDuration >= 0 && colNames[iDuration]) obj[colNames[iDuration]] = fmtDuration(durHrs);
      if (iPostBank >= 0 && colNames[iPostBank]) obj[colNames[iPostBank]] = fmtDuration(calc.postBanking);
      if (iWeekend  >= 0 && colNames[iWeekend])  obj[colNames[iWeekend]]  = fmtDuration(calc.weekend);
      if (iPubHol   >= 0 && colNames[iPubHol])   obj[colNames[iPubHol]]   = fmtDuration(calc.publicHol);
      if (iActual   >= 0 && colNames[iActual])   obj[colNames[iActual]]   = fmtDuration(calc.actual);
      if (iTotal    >= 0 && colNames[iTotal])    obj[colNames[iTotal]]    = fmtDuration(calc.total);
      preview.push(obj);
    }

    const newPct = 20 + Math.round((i / total) * 74);
    if (newPct > lastPct) {
      lastPct = newPct;
      progress(newPct, `Processing ${(i + 1).toLocaleString()} / ${total.toLocaleString()} rows…  (overlaps: ${overlapCt})`);
    }
  }

  progress(95, 'Writing Excel file…');

  // Build output workbook: updated Data sheet + original Calender sheet
  const wbOut = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wbOut, ws, dataSheetName);
  XLSX.utils.book_append_sheet(wbOut, wb.Sheets[calSheetName], calSheetName);

  const xlsxArr = XLSX.write(wbOut, { type: 'array', bookType: 'xlsx' });
  // Slice to get exact ArrayBuffer (avoid sending oversized pooled buffer)
  const xlsxBuf = xlsxArr.buffer.slice(xlsxArr.byteOffset, xlsxArr.byteOffset + xlsxArr.byteLength);

  self.postMessage(
    { type: 'done', xlsx: xlsxBuf, preview, total, overlaps: overlapCt },
    [xlsxBuf]
  );
}

// ─── Message handler ──────────────────────────────────────────────────────────

self.onmessage = function (e) {
  try {
    processDuration(e.data.file);
  } catch (err) {
    self.postMessage({ type: 'error', msg: err.message || String(err) });
  }
};
