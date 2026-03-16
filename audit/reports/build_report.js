#!/usr/bin/env node
/**
 * build_report.js - Data Whisperer Audit Report Generator
 *
 * Uses docx npm library v9.5+ to produce a fully-formatted .docx that matches
 * the brand specification in DataWhisperer_Engine_Prompt.docx.
 *
 * Called automatically by generate_report.py when Node.js is available.
 * Falls back to python-docx renderer if this script exits non-zero.
 *
 * Install dependency (once):
 *   npm install -g docx          # system-wide
 *   # or: cd audit/reports && npm install docx
 *
 * Usage:
 *   node build_report.js --data /path/to/run_data.json --out /path/to/report.docx
 */

'use strict';

const fs   = require('fs');
const path = require('path');

// ---------------------------------------------------------------------------
// Load docx - try global install first, then local node_modules
// ---------------------------------------------------------------------------
let docx;
try {
  docx = require('docx');
} catch (_) {
  try {
    docx = require(path.join(__dirname, 'node_modules', 'docx'));
  } catch (e) {
    console.error('[build_report] docx npm package not found.');
    console.error('  Run: npm install -g docx  (or: cd audit/reports && npm install docx)');
    process.exit(2);
  }
}

const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, HeadingLevel, BorderStyle, WidthType,
  ShadingType, VerticalAlign, PageNumber, PageBreak, LevelFormat,
} = docx;

// ---------------------------------------------------------------------------
// Brand palette - DataWhisperer_Engine_Prompt.docx spec
// ---------------------------------------------------------------------------
const NAVY     = '1B3F6B';   // H1 colour
const BLUE     = '2C5F8A';   // H2 colour, info badge/callout border
const DGRAY    = '444444';   // H3 colour
const BODY     = '333333';   // body text
const WHITE    = 'FFFFFF';
const LGRAY    = 'F2F2F2';   // alternating table row fill
const GREEN    = '006400';   // positive values
const RED      = 'C00000';   // CRITICAL badge, danger callout border, negative
const ORANGE   = 'E06C00';   // HIGH badge
const YELLOW   = 'BF8F00';   // MEDIUM badge
const PURPLE   = '7030A0';   // BUG badge
const FOOTER_C = '888888';   // footer text

const SEV_COLOR = { CRITICAL: RED, HIGH: ORANGE, MEDIUM: YELLOW, BUG: PURPLE, INFO: BLUE };

// Page: US Letter, 0.75" margins (1080 DXA = 0.75 in)
const PAGE_W     = 12240;
const PAGE_H     = 15840;
const MARGIN     = 1080;
const CONTENT_W  = PAGE_W - MARGIN * 2;   // 10 080 DXA

// Table cell borders
const BORDER    = { style: BorderStyle.SINGLE, size: 1, color: 'CCCCCC' };
const BORDERS   = { top: BORDER, bottom: BORDER, left: BORDER, right: BORDER };
const NO_BORDER = { style: BorderStyle.NONE,   size: 0, color: 'FFFFFF' };
const NO_BORDERS = { top: NO_BORDER, bottom: NO_BORDER, left: NO_BORDER, right: NO_BORDER };

// ---------------------------------------------------------------------------
// Plain-English translation of internal engine reason flags
// ---------------------------------------------------------------------------
function translateReason (reason) {
  if (!reason || String(reason).trim() === '' || reason === 'nan') return 'No reason recorded';
  const r = String(reason).trim();

  // Pipe-separated multi-reason (REVIEW records): translate each part
  if (r.includes('|')) {
    return r.split('|').map(p => translateReason(p.trim())).filter(Boolean).join('; ');
  }

  // Strip "reject_match:" prefix (REJECT_MATCH tier reasons)
  const rInner = r.replace(/^reject_match:/i, '').trim();

  // Strip leading "fix_type:" prefix (REVIEW reasons like "salary:below_threshold")
  const rNoPrefix = rInner.replace(/^[a-z_]+:/, '').trim();

  // dob_name_low_confidence (0.600<0.75) - extract actual score AND minimum threshold
  const dobFull = rInner.match(/^dob_name_low_confidence\s*\(?([\d.]+)<([\d.]+)/);
  if (dobFull) {
    const pctScore  = Math.round(parseFloat(dobFull[1]) * 100);
    const pctThresh = Math.round(parseFloat(dobFull[2]) * 100);
    return `Matched on name and date of birth only - confidence score of ${pctScore}% fell below the ${pctThresh}% minimum required to trust the match.`;
  }

  // Legacy: dob_name_low_confidence (0.600) without threshold
  const dobM = rInner.match(/^dob_name_low_confidence\s*\(?([\d.]+)/);
  if (dobM) {
    const pct = Math.round(parseFloat(dobM[1]) * 100);
    return `Matched on name and date of birth only - confidence too low to trust (${pct}%)`;
  }

  // fuzzy_extreme_salary_ratio (2.5000>2.5) - wrong-person signal
  const fuzzyM = rInner.match(/^fuzzy_extreme_salary_ratio\s*\(?([\d.]+)/);
  if (fuzzyM) {
    const ratio = parseFloat(fuzzyM[1]);
    return `Salary is ${ratio.toFixed(1)}x different between systems on a fuzzy match - flagged as possible wrong-person pairing`;
  }

  // salary_ratio_extreme (2.0000 outside [0.85, 1.15])
  const salM = (rNoPrefix.match(/^salary_ratio_extreme\s*\(?([\d.]+)/) ||
                r.match(/^salary_ratio_extreme\s*\(?([\d.]+)/));
  if (salM) {
    const ratio = parseFloat(salM[1]);
    if (ratio > 1.5) return 'Salary more than doubled between systems - needs human review';
    if (ratio < 0.5) return 'Salary dropped by more than half between systems - needs human review';
    return 'Salary changed significantly between systems - needs human review';
  }

  // below_threshold (0.82<0.97) - confidence below required minimum
  const belowM = rNoPrefix.match(/^below_threshold\s*\(?([\d.]+)<([\d.]+)/);
  if (belowM) {
    const pctScore  = Math.round(parseFloat(belowM[1]) * 100);
    const pctThresh = Math.round(parseFloat(belowM[2]) * 100);
    return `Confidence score of ${pctScore}% fell below the ${pctThresh}% minimum required for this field - needs human review`;
  }

  // low_confidence (0.82) single value
  const lowM = rNoPrefix.match(/^low_confidence\s*\(?([\d.]+)/);
  if (lowM) {
    const pct = Math.round(parseFloat(lowM[1]) * 100);
    return `Confidence score too low (${pct}%) - needs human review`;
  }

  // active_to_terminated
  if (/^active_to_terminated/.test(rNoPrefix)) {
    return 'Status changed from active to terminated - needs human review';
  }

  const EXACT = {
    'hire_date:year_shift_with_other_mismatches':
      'Start date changed by a full year, plus other fields also changed',
    'hire_date:off_by_one_day_pattern':
      'Start date off by one day (likely a system convention difference - auto-approved)',
    'hire_date:year_shift_systematic':
      'Start date changed by one year - appears to be a systematic pattern (auto-approved)',
    'hire_date:off_by_one_year_systematic':
      'Start date off by one year across many records - auto-approved as systematic',
    'hire_date:off_by_one_year_pattern':
      'Start date off by exactly one year (likely a system import convention - auto-approved)',
    'worker_id_auto_approve':  'Exact ID match - auto-approved',
    'pk_auto_approve':         'Matched on name, date of birth, and last-4 identifier - auto-approved',
    'active_to_terminated':    'Status changed from active to terminated - needs human review',
    'hire_date_wave':          'Start date matches a bulk import date shared by many other employees - needs human review',
    'name_change_detected':    'Last name differs between systems - needs human review to confirm same employee',
  };

  // name_change_detected (old_last -> new_last) parametric form
  const ncM = rNoPrefix.match(/^name_change_detected\s*\(([^)]+)\)/);
  if (ncM) {
    return `Last name changed from "${ncM[1].trim()}" - needs human review to confirm same employee`;
  }

  // Exact lookups on rInner, rNoPrefix, then full r
  for (const candidate of [rInner, rNoPrefix, r]) {
    if (EXACT[candidate]) return EXACT[candidate];
  }
  // Prefix lookups
  for (const [key, val] of Object.entries(EXACT)) {
    if (rInner.startsWith(key) || rNoPrefix.startsWith(key) || r.startsWith(key)) return val;
  }

  // Generic fallback: humanise from most-stripped form
  const display = rNoPrefix || rInner || r;
  return display.replace(/hire_date:/g, 'start date: ').replace(/_/g, ' ').trim();
}

function translateMatchSource (source) {
  const MAP = {
    'worker_id':      'Exact employee ID match',
    'pk':             'Matched on name, date of birth, and last-4',
    'last4_dob':      'Matched on last-4 SSN and date of birth',
    'dob_name':       'Matched on name and date of birth (high-risk tier)',
    'name_hire_date': 'Matched on name and start date',
    'recon_id':       'Exact reconciliation ID match',
  };
  return MAP[(source || '').trim().toLowerCase()] || String(source || '');
}

function displayName (row) {
  // Returns 'First Last' title-cased from name components, or falls back to full_name_norm.
  const first = (row.old_first_name_norm || '').trim();
  const last  = (row.old_last_name_norm  || '').trim();
  const toTitle = s => s.split(/\s+/).map(w => w ? w[0].toUpperCase() + w.slice(1) : '').join(' ');
  if (first && last) return toTitle(first) + ' ' + toTitle(last);
  const full = (row.old_full_name_norm || '').trim();
  if (full) return toTitle(full);
  return row.pair_id || '-';
}

// ---------------------------------------------------------------------------
// Parse CLI args
// ---------------------------------------------------------------------------
function parseArgs () {
  const argv = process.argv.slice(2);
  const r = {};
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--data' && argv[i + 1]) r.data = argv[++i];
    if (argv[i] === '--out'  && argv[i + 1]) r.out  = argv[++i];
  }
  return r;
}

// ---------------------------------------------------------------------------
// Low-level helpers
// ---------------------------------------------------------------------------
const txt = (text, opts = {}) =>
  new TextRun({ text: String(text ?? ''), font: 'Arial', ...opts });

const spacer = () =>
  new Paragraph({ children: [txt('')], spacing: { before: 60, after: 60 } });

const pgBreak = () =>
  new Paragraph({ children: [new PageBreak()] });

function body (text) {
  return new Paragraph({
    children:  [txt(text, { color: BODY, size: 20 })],
    spacing:   { before: 60, after: 80 },
  });
}

function h1 (text) {
  return new Paragraph({
    heading:  HeadingLevel.HEADING_1,
    children: [txt(text, { color: NAVY, bold: true, size: 34 })],
    spacing:  { before: 320, after: 160 },
  });
}

function h2 (text) {
  return new Paragraph({
    heading:  HeadingLevel.HEADING_2,
    children: [txt(text, { color: BLUE, bold: true, size: 26 })],
    spacing:  { before: 240, after: 120 },
    border:   { bottom: { style: BorderStyle.SINGLE, size: 6, color: BLUE, space: 1 } },
  });
}

function h3 (text) {
  return new Paragraph({
    heading:  HeadingLevel.HEADING_3,
    children: [txt(text, { color: DGRAY, bold: true, size: 22 })],
    spacing:  { before: 180, after: 80 },
  });
}

function bullet (text) {
  return new Paragraph({
    numbering: { reference: 'bullets', level: 0 },
    children:  [txt(text, { color: BODY, size: 20 })],
  });
}

// ---------------------------------------------------------------------------
// Callout: left-accent table simulating a bordered callout box
// ---------------------------------------------------------------------------
function callout (text, type = 'warning') {
  const fills = {
    warning: { fill: 'FFF9E6', border: 'D6B656' },
    info:    { fill: 'E8F4FD', border: BLUE      },
    danger:  { fill: 'FCE4E4', border: RED        },
  };
  const { fill, border } = fills[type] || fills.warning;
  const accentW = 180;
  const textW   = CONTENT_W - accentW;

  return new Table({
    width:        { size: CONTENT_W, type: WidthType.DXA },
    columnWidths: [accentW, textW],
    borders:      NO_BORDERS,
    rows: [new TableRow({ children: [
      new TableCell({
        width:    { size: accentW, type: WidthType.DXA },
        borders:  NO_BORDERS,
        shading:  { fill: border, type: ShadingType.CLEAR },
        children: [new Paragraph({ children: [txt('')] })],
      }),
      new TableCell({
        width:   { size: textW, type: WidthType.DXA },
        borders: NO_BORDERS,
        shading: { fill, type: ShadingType.CLEAR },
        margins: { top: 120, bottom: 120, left: 200, right: 200 },
        children: [new Paragraph({
          children: [txt(text, { color: BODY, size: 20 })],
          spacing:  { before: 40, after: 40 },
        })],
      }),
    ]})],
  });
}

// ---------------------------------------------------------------------------
// KV table: 2-column label / value with alternating rows
// ---------------------------------------------------------------------------
function kvTable (pairs) {
  const lW = 3500;
  const vW = CONTENT_W - lW;   // 6 580

  return new Table({
    width:        { size: CONTENT_W, type: WidthType.DXA },
    columnWidths: [lW, vW],
    rows: pairs.map(([label, value], i) => new TableRow({ children: [
      new TableCell({
        width:   { size: lW, type: WidthType.DXA },
        borders: BORDERS,
        shading: { fill: i % 2 === 0 ? LGRAY : WHITE, type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 140, right: 140 },
        children: [new Paragraph({ children: [txt(String(label), { color: BODY, bold: true, size: 20 })] })],
      }),
      new TableCell({
        width:   { size: vW, type: WidthType.DXA },
        borders: BORDERS,
        shading: { fill: i % 2 === 0 ? LGRAY : WHITE, type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 140, right: 140 },
        children: [new Paragraph({ children: [txt(String(value ?? ''), { color: BODY, size: 20 })] })],
      }),
    ]})),
  });
}

// ---------------------------------------------------------------------------
// Data table: navy header row + alternating data rows
// ---------------------------------------------------------------------------
function dataTable (headers, rows, colWidths) {
  const n  = headers.length;
  const ws = colWidths || Array(n).fill(Math.floor(CONTENT_W / n));

  const headerRow = new TableRow({ children: headers.map((h, i) =>
    new TableCell({
      width:   { size: ws[i], type: WidthType.DXA },
      borders: BORDERS,
      shading: { fill: NAVY, type: ShadingType.CLEAR },
      margins: { top: 80, bottom: 80, left: 140, right: 140 },
      children: [new Paragraph({ children: [txt(h, { color: WHITE, bold: true, size: 20 })] })],
    })
  )});

  const dataRows = rows.map((row, ri) => new TableRow({ children: row.map((val, ci) =>
    new TableCell({
      width:   { size: ws[ci], type: WidthType.DXA },
      borders: BORDERS,
      shading: { fill: ri % 2 === 0 ? LGRAY : WHITE, type: ShadingType.CLEAR },
      margins: { top: 80, bottom: 80, left: 140, right: 140 },
      children: [new Paragraph({ children: [txt(String(val ?? ''), { color: BODY, size: 20 })] })],
    })
  )}));

  return new Table({
    width:        { size: CONTENT_W, type: WidthType.DXA },
    columnWidths: ws,
    rows:         [headerRow, ...dataRows],
  });
}

// ---------------------------------------------------------------------------
// 4-column stats box used on cover / executive summary
// ---------------------------------------------------------------------------
function statsBox (cols) {
  // cols: [{label, value, sub?}]  - up to 4 entries
  const n  = cols.length;
  const cW = Math.floor(CONTENT_W / n);
  const ws = [...Array(n - 1).fill(cW), CONTENT_W - cW * (n - 1)];

  return new Table({
    width:        { size: CONTENT_W, type: WidthType.DXA },
    columnWidths: ws,
    rows: [new TableRow({ children: cols.map(({ label, value }, i) =>
      new TableCell({
        width:         { size: ws[i], type: WidthType.DXA },
        borders:       BORDERS,
        shading:       { fill: NAVY, type: ShadingType.CLEAR },
        margins:       { top: 140, bottom: 140, left: 140, right: 140 },
        verticalAlign: VerticalAlign.CENTER,
        children: [
          new Paragraph({ alignment: AlignmentType.CENTER,
            children: [txt(label, { color: WHITE, bold: true, size: 18 })] }),
          new Paragraph({ alignment: AlignmentType.CENTER,
            children: [txt(value, { color: WHITE, bold: true, size: 28 })] }),
        ],
      })
    )})],
  });
}

// ---------------------------------------------------------------------------
// Findings summary table row (with severity badge cell)
// ---------------------------------------------------------------------------
function findingsTable (findings) {
  const bW = 1400;   // badge
  const tW = 4000;   // title
  const cW = 1200;   // count
  const iW = CONTENT_W - bW - tW - cW;  // 3 480

  const hdr = new TableRow({ children:
    ['Severity', 'Finding', 'Count', 'Impact'].map((h, i) =>
      new TableCell({
        width:   { size: [bW, tW, cW, iW][i], type: WidthType.DXA },
        borders: BORDERS,
        shading: { fill: NAVY, type: ShadingType.CLEAR },
        margins: { top: 80, bottom: 80, left: 140, right: 140 },
        children: [new Paragraph({ children: [txt(h, { color: WHITE, bold: true, size: 20 })] })],
      })
    )
  });

  const rows = findings.map((f, ri) => new TableRow({ children: [
    // Severity badge
    new TableCell({
      width:         { size: bW, type: WidthType.DXA },
      borders:       BORDERS,
      shading:       { fill: SEV_COLOR[f.severity] || BLUE, type: ShadingType.CLEAR },
      margins:       { top: 80, bottom: 80, left: 140, right: 140 },
      verticalAlign: VerticalAlign.CENTER,
      children: [new Paragraph({ alignment: AlignmentType.CENTER,
        children: [txt(f.severity, { color: WHITE, bold: true, size: 18 })] })],
    }),
    // Title
    new TableCell({
      width:   { size: tW, type: WidthType.DXA },
      borders: BORDERS,
      shading: { fill: ri % 2 === 0 ? LGRAY : WHITE, type: ShadingType.CLEAR },
      margins: { top: 80, bottom: 80, left: 140, right: 140 },
      children: [new Paragraph({ children: [txt(f.title, { color: BODY, bold: true, size: 20 })] })],
    }),
    // Count
    new TableCell({
      width:   { size: cW, type: WidthType.DXA },
      borders: BORDERS,
      shading: { fill: ri % 2 === 0 ? LGRAY : WHITE, type: ShadingType.CLEAR },
      margins: { top: 80, bottom: 80, left: 140, right: 140 },
      children: [new Paragraph({ alignment: AlignmentType.RIGHT,
        children: [txt(Number(f.count).toLocaleString(), { color: BODY, size: 20 })] })],
    }),
    // Impact
    new TableCell({
      width:   { size: iW, type: WidthType.DXA },
      borders: BORDERS,
      shading: { fill: ri % 2 === 0 ? LGRAY : WHITE, type: ShadingType.CLEAR },
      margins: { top: 80, bottom: 80, left: 140, right: 140 },
      children: [new Paragraph({ children: [txt(f.impact, { color: BODY, size: 20 })] })],
    }),
  ]}));

  return new Table({
    width:        { size: CONTENT_W, type: WidthType.DXA },
    columnWidths: [bW, tW, cW, iW],
    rows:         [hdr, ...rows],
  });
}

// ---------------------------------------------------------------------------
// Section: Cover / Title page
// ---------------------------------------------------------------------------
function buildCover (data) {
  return [
    spacer(), spacer(), spacer(),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      children:  [txt('DATA WHISPERER', { color: NAVY, bold: true, size: 64 })],
      spacing:   { before: 200, after: 120 },
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      children:  [txt('Reconciliation Audit Report', { color: BLUE, bold: true, size: 40 })],
      spacing:   { before: 60, after: 400 },
    }),
    kvTable([
      ['Run date',         data.run_date ?? ''],
      ['Source database',  data.db_name  ?? ''],
      ['Total records',    Number(data.total_records || 0).toLocaleString()],
      ['Generated by',     'Data Whisperer Engine'],
    ]),
    spacer(), spacer(),
    new Paragraph({
      alignment: AlignmentType.CENTER,
      children:  [txt(
        'Before AI tells you the story, make sure the data is correct.',
        { color: FOOTER_C, italics: true, size: 20 }
      )],
      spacing: { before: 300, after: 60 },
    }),
    pgBreak(),
  ];
}

// ---------------------------------------------------------------------------
// Section: Executive Summary
// ---------------------------------------------------------------------------
function buildExecutiveSummary (data) {
  const total    = data.total_records || 0;
  const a        = data.actions       || {};
  const nApprove = Number(a.APPROVE       || 0);
  const nReview  = Number(a.REVIEW        || 0);
  const nReject  = Number(a.REJECT_MATCH  || 0);
  const nAZ      = Number(data.active_zero_count || 0);
  const sgPass   = data.sanity_gate?.passed !== false;

  // Opening paragraph + plain-English bullet list
  const openPara = new Paragraph({
    children: [txt(
      `We compared ${Number(total).toLocaleString()} employee records between the source system ` +
      `and the new system. Here is what we found:`,
      { color: BODY, size: 20 }
    )],
    spacing: { before: 60, after: 80 },
  });

  const bulletItems = [
    `${nApprove.toLocaleString()} records look correct and are ready to load - no action needed.`,
    `${nReview.toLocaleString()} records need a human to review them before they go in.`,
  ];
  if (nReject > 0) bulletItems.push(
    `${nReject.toLocaleString()} records were flagged as possible wrong-person matches and blocked entirely.`
  );
  if (nAZ > 0) bulletItems.push(
    `${nAZ.toLocaleString()} employees are showing $0 salary in the new system - ` +
    `this needs to be fixed before anyone gets paid.`
  );

  const bulletParas = bulletItems.map(t => bullet(t));

  const whatNextHead = h2('What happens next');
  const whatNextBody = body(
    `The correction files attached to this report are ready to load into the new system for all ` +
    `${nApprove.toLocaleString()} auto-approved records. Before loading, a reviewer must work through ` +
    `the ${nReview.toLocaleString()} records in the review queue and confirm each one. ` +
    `Once the review queue is cleared, a final corrections run can be executed.`
  );

  const elems = [
    h1('1. Executive Summary'),
    openPara,
    ...bulletParas,
    spacer(),
    statsBox([
      { label: 'RECORDS',       value: Number(total).toLocaleString()    },
      { label: 'AUTO-APPROVED', value: nApprove.toLocaleString()         },
      { label: 'REVIEW',        value: nReview.toLocaleString()          },
      { label: 'SANITY GATE',   value: sgPass ? '✓  PASS' : '✗  FAIL'   },
    ]),
    spacer(),
    whatNextHead,
    whatNextBody,
    spacer(),
  ];

  if (nAZ > 0) {
    elems.push(callout(
      `Action required before payroll: ${nAZ.toLocaleString()} active employees have $0 salary ` +
      `in the new system. Salary corrections for these records are blocked until the source data is corrected.`,
      'danger'
    ));
    elems.push(spacer());
  }

  if (nReject > 0) {
    elems.push(callout(
      `${nReject.toLocaleString()} records were blocked from corrections entirely - these appear to be ` +
      `wrong-person matches and need manual investigation. Do not load corrections for these records.`,
      'warning'
    ));
    elems.push(spacer());
  }

  elems.push(pgBreak());
  return elems;
}

// ---------------------------------------------------------------------------
// Section: Findings Summary
// ---------------------------------------------------------------------------
function buildFindingsSummary (data) {
  const elems = [
    h1('2. Findings Summary'),
    body('All findings detected in this reconciliation run, sorted by severity.'),
    spacer(),
  ];

  if (!data.findings || data.findings.length === 0) {
    elems.push(callout('No significant findings detected. The reconciliation run passed all checks.', 'info'));
  } else {
    elems.push(findingsTable(data.findings));
  }

  elems.push(spacer());
  elems.push(pgBreak());
  return elems;
}

// ---------------------------------------------------------------------------
// Section: Match Quality Analysis
// ---------------------------------------------------------------------------
function buildMatchQuality (data) {
  const total = data.total_records || 1;
  const elems = [h1('3. Match Quality Analysis'), h2('3.1 Match Source Breakdown'), spacer()];

  if (data.match_sources && data.match_sources.length > 0) {
    elems.push(dataTable(
      ['Match Source', 'Count', '% of Total'],
      data.match_sources.map(([s, c]) => [s, Number(c).toLocaleString(), `${(c / total * 100).toFixed(1)}%`]),
      [3000, 2000, 5080],
    ));
    elems.push(spacer());
  }

  if (data.confidence_bands && Object.keys(data.confidence_bands).length > 0) {
    elems.push(h2('3.2 Confidence Distribution'));
    elems.push(spacer());
    const cb = data.confidence_bands;
    elems.push(dataTable(
      ['Confidence Band', 'Count', '% of Total'],
      [
        ['Exact (1.00)',          Number(cb.exact   || 0).toLocaleString(), `${((cb.exact   || 0) / total * 100).toFixed(1)}%`],
        ['High (0.97-0.99)',      Number(cb.high    || 0).toLocaleString(), `${((cb.high    || 0) / total * 100).toFixed(1)}%`],
        ['Medium (0.80-0.96)',    Number(cb.medium  || 0).toLocaleString(), `${((cb.medium  || 0) / total * 100).toFixed(1)}%`],
        ['Low (< 0.80)',          Number(cb.low     || 0).toLocaleString(), `${((cb.low     || 0) / total * 100).toFixed(1)}%`],
        ['Missing / not scored',  Number(cb.missing || 0).toLocaleString(), `${((cb.missing || 0) / total * 100).toFixed(1)}%`],
      ],
      [4000, 2000, 4080],
    ));
    if ((cb.low || 0) > 0) {
      elems.push(spacer());
      elems.push(callout(
        `${Number(cb.low).toLocaleString()} pairs have low confidence scores (< 0.80). These require careful human review before any corrections are applied.`,
        'warning'
      ));
    }
    elems.push(spacer());
  }

  elems.push(pgBreak());
  return elems;
}

// ---------------------------------------------------------------------------
// Section: Data Quality Findings
// ---------------------------------------------------------------------------
function buildDataQuality (data) {
  const elems = [h1('4. Data Quality Findings'), h2('4.1 Active Employees with $0 Salary'), spacer()];

  if ((data.active_zero_count || 0) > 0) {
    elems.push(callout(
      `${Number(data.active_zero_count).toLocaleString()} active employees have $0 or missing salary. ` +
      `Salary corrections for these records have been blocked from the corrections pipeline.`,
      'danger'
    ));
    if (data.active_zero_sample && data.active_zero_sample.length > 0) {
      elems.push(spacer());
      elems.push(h3('Sample Records'));
      const sampleHdrs = ['Pair ID', 'Employee Name', 'Old Salary', 'New Salary'].slice(0, data.active_zero_sample[0].length);
      elems.push(dataTable(sampleHdrs, data.active_zero_sample.slice(0, 10), [2000, 3000, 2000, 3080].slice(0, sampleHdrs.length)));
    }
  } else {
    elems.push(new Paragraph({ children: [txt('No active employees with $0 or missing salary detected.', { color: GREEN, bold: true, size: 20 })] }));
  }

  const spf = data.salary_parse_failures || {};
  if ((spf.old_count || 0) > 0) {
    const samples = (spf.old_samples || []).slice(0, 10).join(', ') || '(none captured)';
    elems.push(spacer());
    elems.push(callout(
      `${Number(spf.old_count).toLocaleString()} salary values in the old system could not be parsed and were treated as missing. ` +
      `Sample unparseable values: ${samples}. These records may have salary corrections blocked or require manual review.`,
      'warning'
    ));
  }
  if ((spf.new_count || 0) > 0) {
    const samples = (spf.new_samples || []).slice(0, 10).join(', ') || '(none captured)';
    elems.push(spacer());
    elems.push(callout(
      `${Number(spf.new_count).toLocaleString()} salary values in the new system could not be parsed and were treated as missing. ` +
      `Sample unparseable values: ${samples}. These records may have salary corrections blocked or require manual review.`,
      'warning'
    ));
  }

  elems.push(spacer());
  elems.push(h2('4.2 Hire Date Wave Detection'));
  elems.push(spacer());

  if (data.wave_dates && data.wave_dates.length > 0) {
    elems.push(callout(
      `${Number(data.hire_date_stats?.n_wave || 0).toLocaleString()} records share hire dates that appear in a concentrated cluster - ` +
      `indicative of a bulk import. These have been routed to REVIEW.`,
      'warning'
    ));
    elems.push(spacer());
    elems.push(dataTable(['Hire Date', 'Count'], data.wave_dates.slice(0, 10).map(([d, c]) => [d, Number(c).toLocaleString()]), [4000, 6080]));
  } else {
    elems.push(new Paragraph({ children: [txt('No hire date wave patterns detected.', { color: GREEN, bold: true, size: 20 })] }));
  }

  elems.push(spacer());
  elems.push(h2('4.3 Rejected Match Pairings (REJECT_MATCH)'));
  elems.push(spacer());

  const rm = data.reject_matches || {};
  if ((rm.total || 0) > 0) {
    elems.push(callout(
      `${Number(rm.total).toLocaleString()} pairs were identified as likely wrong-person pairings and flagged REJECT_MATCH. ` +
      `No automated corrections will be applied.`,
      'danger'
    ));
    if (rm.reasons && rm.reasons.length > 0) {
      elems.push(spacer());
      elems.push(dataTable(['Rejection Reason', 'Count'], rm.reasons.map(([r, c]) => [r, Number(c).toLocaleString()]), [7000, 3080]));
    }
  } else {
    elems.push(new Paragraph({ children: [txt('No REJECT_MATCH records detected. All pairings appear valid.', { color: GREEN, bold: true, size: 20 })] }));
  }

  elems.push(spacer());
  elems.push(pgBreak());
  return elems;
}

// ---------------------------------------------------------------------------
// Section: Field Change Analysis
// ---------------------------------------------------------------------------
function buildFieldChanges (data) {
  const elems = [h1('5. Field Change Analysis'), spacer()];

  if (data.change_summary && data.change_summary.length > 0) {
    elems.push(dataTable(['Change Type', 'Count', '% of Total'], data.change_summary, [4000, 2000, 4080]));
    elems.push(spacer());
  }

  // 5.1 Salary
  elems.push(h2('5.1 Salary Changes'));
  elems.push(spacer());
  const sal = data.salary;
  if (sal) {
    const kvRows = [['Salary changes (total)', Number(sal.total || 0).toLocaleString()]];
    if ((sal.n_excluded_active_zero || 0) > 0) {
      kvRows.push([
        '  - Active/$0 excl. from stats',
        `${Number(sal.n_excluded_active_zero).toLocaleString()}  (data quality - not real changes)`,
      ]);
    }
    const fmt = (n) => {
      const v = Number(n || 0);
      return `$${Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}${v < 0 ? ' (decrease)' : ''}`;
    };
    kvRows.push(
      ['  - Included in stats',    Number(sal.n_included   || 0).toLocaleString()],
      ['Increases',                Number(sal.n_increase   || 0).toLocaleString()],
      ['Decreases',                Number(sal.n_decrease   || 0).toLocaleString()],
      ['Mean delta',               `$${Number(sal.mean_delta   || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`],
      ['Median delta',             `$${Number(sal.median_delta || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`],
      ['Largest increase',         fmt(sal.max_increase)],
      ['Largest decrease',         fmt(sal.max_decrease)],
    );
    elems.push(kvTable(kvRows));
  } else {
    elems.push(body('(salary_delta column not available)'));
  }
  elems.push(spacer());

  // 5.2 Status changes
  elems.push(h2('5.2 Status Changes'));
  elems.push(spacer());
  if (data.status_transitions && data.status_transitions.length > 0) {
    elems.push(dataTable(['Transition', 'Count'], data.status_transitions.slice(0, 10).map(([t, c]) => [t, Number(c).toLocaleString()]), [7000, 3080]));
  } else {
    elems.push(body('No status changes detected.'));
  }
  elems.push(spacer());

  // 5.3 Hire date changes - plain-English summary (no technical pattern table)
  elems.push(h2('5.3 Hire Date Changes'));
  elems.push(spacer());
  const hd = data.hire_date_stats || {};
  if ((hd.total || 0) > 0) {
    const nTotal   = Number(hd.total || 0);
    const nAuto    = Number(hd.n_auto_approved   || 0);
    const nYearRev = Number(hd.n_year_review     || 0);
    const nSys     = Number(hd.n_systematic      || 0);
    const nOther   = Math.max(0, nTotal - nAuto - nYearRev - nSys);
    const parts    = [];
    if (nAuto    > 0) parts.push(`${nAuto.toLocaleString()} were automatically approved because they matched known system conversion patterns (such as off-by-one-day differences)`);
    if (nYearRev > 0) parts.push(`${nYearRev.toLocaleString()} had a full year shift combined with other changes and were sent to review`);
    if (nSys     > 0) parts.push(`${nSys.toLocaleString()} appear to be a systematic date format difference and were auto-approved`);
    if (nOther   > 0) parts.push(`${nOther.toLocaleString()} had other hire date differences flagged for review`);
    const summary = parts.length > 0
      ? `Of ${nTotal.toLocaleString()} hire date differences found: ${parts.join('; ')}.`
      : `A total of ${nTotal.toLocaleString()} records had hire date differences.`;
    elems.push(body(summary));
  } else {
    elems.push(body('No hire date differences detected.'));
  }
  elems.push(spacer());

  elems.push(pgBreak());
  return elems;
}

// ---------------------------------------------------------------------------
// Section: Priority Review Queue
// ---------------------------------------------------------------------------
function buildReviewQueue (data) {
  const rq    = data.review_queue || {};
  const total = rq.total || 0;
  const elems = [
    h1('6. Priority Review Queue'),
    body(
      `A total of ${Number(total).toLocaleString()} records need a human reviewer to look at them ` +
      `before corrections can be applied. The table below shows the 10 highest-priority items.`
    ),
    spacer(),
  ];

  if (total === 0) {
    elems.push(callout('No records require human review. All approved records are ready to load.', 'info'));
  } else {
    if (rq.top_items && rq.top_items.length > 0) {
      // top_items: [name, reason, fix_types] - translate reason column
      const hdrs = ['Employee', 'Why It Needs Review', 'Fields Changed'];
      const ws   = [2200, 5300, 2580];
      const rows = rq.top_items.slice(0, 10).map(item => {
        const [name, reason, fixTypes] = Array.isArray(item) ? item : [item.name, item.reason, item.fix_types];
        return [
          String(name   || '-'),
          translateReason(String(reason   || '')),
          String(fixTypes || '-').replace(/\|/g, ', '),
        ];
      });
      elems.push(dataTable(hdrs, rows, ws));
      if (total > 10) {
        elems.push(spacer());
        elems.push(body(`Showing 10 of ${Number(total).toLocaleString()} total review items. See the review_queue.csv file for the complete list.`));
      }
    }
    if (rq.by_fix_type && rq.by_fix_type.length > 0) {
      elems.push(spacer());
      elems.push(h3('Review Queue by Change Type'));
      elems.push(dataTable(
        ['Change Type', 'Records Needing Review'],
        rq.by_fix_type.map(([ft, c]) => [ft, Number(c).toLocaleString()]),
        [5000, 5080]
      ));
    }
  }

  elems.push(spacer());
  elems.push(pgBreak());
  return elems;
}

// ---------------------------------------------------------------------------
// Section: Rejected Matches
// ---------------------------------------------------------------------------
function buildRejectedMatches (data) {
  const rm    = data.reject_matches || {};
  const elems = [h1('7. Blocked Records - Possible Wrong-Person Matches'), spacer()];

  if ((rm.total || 0) === 0) {
    elems.push(callout('No records were blocked. All pairings appear to be correct person matches.', 'info'));
  } else {
    elems.push(body(
      `${Number(rm.total).toLocaleString()} records were blocked from corrections because the matching engine ` +
      `detected they were likely wrong-person pairings - the system matched an employee to someone else in ` +
      `the other file. These records have been completely excluded from all correction files and must be ` +
      `investigated and re-matched manually.`
    ));
    elems.push(spacer());
    elems.push(body(
      `The most common reason for a block is a name-and-date-of-birth-only match where the confidence score ` +
      `fell below the minimum acceptable threshold. These low-confidence matches carry a real risk of applying ` +
      `salary, status, or hire-date changes to the wrong person.`
    ));
    elems.push(spacer());
    if (rm.by_source && rm.by_source.length > 0) {
      elems.push(dataTable(
        ['How They Were Originally Matched', 'Count Blocked'],
        rm.by_source.map(([s, c]) => [translateMatchSource(s), Number(c).toLocaleString()]),
        [6000, 4080]
      ));
    }
  }

  elems.push(spacer());
  elems.push(pgBreak());
  return elems;
}

// ---------------------------------------------------------------------------
// Section: Sanity Gate Analysis
// ---------------------------------------------------------------------------
function buildSanityGate (data) {
  const sg    = data.sanity_gate || {};
  const pass  = sg.passed !== false;
  const elems = [
    h1('8. Sanity Gate Analysis'),
    body('The Sanity Gate evaluates pipeline-level data quality metrics before corrections are applied to the target system.'),
    spacer(),
  ];

  if (sg.metrics && sg.metrics.length > 0) {
    elems.push(dataTable(
      ['Metric', 'Value', 'Threshold', 'Status'],
      sg.metrics.map((m) => [m.name, m.value, m.threshold || '-', m.passed ? '✓  PASS' : '✗  FAIL']),
      [3000, 2000, 2500, 2580],
    ));
    elems.push(spacer());
  }

  elems.push(callout(
    pass
      ? 'The Sanity Gate PASSED. The corrections pipeline may proceed.'
      : 'The Sanity Gate FAILED. Do not apply corrections until all failing metrics are resolved.',
    pass ? 'info' : 'danger'
  ));

  elems.push(spacer());
  elems.push(pgBreak());
  return elems;
}

// ---------------------------------------------------------------------------
// Section: Action Plan
// ---------------------------------------------------------------------------
function buildActionPlan (data) {
  const az  = data.active_zero_count  || 0;
  const rm  = data.reject_matches?.total || 0;
  const rev = data.review_queue?.total   || 0;

  const elems = [
    h1('9. Action Plan'),
    body('Recommended actions based on the findings in this report.'),
    spacer(),
    h2('Immediate Actions - Before Load'),
    spacer(),
  ];

  if (az > 0)  elems.push(bullet(`Fix ${Number(az).toLocaleString()} active employees who show $0 salary in the new system - this is a data extraction issue that must be resolved before payroll runs.`));
  if (rm > 0)  elems.push(bullet(`Investigate ${Number(rm).toLocaleString()} blocked records - these appear to be wrong-person matches that need manual review and re-matching before the migration can complete.`));
  if (rev > 0) elems.push(bullet(`Work through the review queue: ${Number(rev).toLocaleString()} records need a human to look at them and confirm they are correct. Some corrections were held and not applied automatically - these are in the held_corrections file and require manual approval before they can be loaded into the new system.`));
  if (!az && !rm && !rev) elems.push(bullet('All critical checks passed. The correction files are ready to load into the new system.'));

  elems.push(spacer());
  elems.push(h2('Short-term Actions - Data Cleanup'));
  elems.push(spacer());
  elems.push(bullet('Re-run the reconciliation after source data corrections are applied to verify the fixes resolved the flagged issues.'));
  if (rm > 0) elems.push(bullet('Review the matching configuration to reduce wrong-person pairings in future runs - particularly for records matched only on name and date of birth.'));

  elems.push(spacer());
  elems.push(h2('Strategic Recommendations - Next Run'));
  elems.push(spacer());
  elems.push(bullet('Implement pre-migration data validation in the source system to eliminate $0 salary records.'));
  elems.push(bullet('Establish a scheduled reconciliation cadence to catch data drift between migration phases.'));
  elems.push(bullet('Consider tightening the auto-approve salary ratio from ±15% to ±10% if extreme cases persist.'));

  elems.push(spacer(), spacer());
  elems.push(new Paragraph({
    alignment: AlignmentType.CENTER,
    children:  [txt(
      'Data Whisperer  ·  Before AI tells you the story, make sure the data is correct.',
      { color: FOOTER_C, italics: true, size: 18 }
    )],
    spacing: { before: 400, after: 60 },
  }));

  return elems;
}

// ---------------------------------------------------------------------------
// Assemble and write the document
// ---------------------------------------------------------------------------
async function main () {
  const args = parseArgs();
  if (!args.data || !args.out) {
    console.error('[build_report] Usage: node build_report.js --data DATA.json --out OUTPUT.docx');
    process.exit(1);
  }

  let data;
  try {
    data = JSON.parse(fs.readFileSync(args.data, 'utf8'));
  } catch (e) {
    console.error(`[build_report] Failed to read data file: ${e.message}`);
    process.exit(1);
  }

  const footer = new Footer({
    children: [new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [
        txt('Data Whisperer  |  Reconciliation Audit Report  |  Confidential  |  Page ', { color: FOOTER_C, size: 18 }),
        new TextRun({ children: [PageNumber.CURRENT], color: FOOTER_C, size: 18, font: 'Arial' }),
      ],
    })],
  });

  const numbering = {
    config: [{
      reference: 'bullets',
      levels: [{
        level:     0,
        format:    LevelFormat.BULLET,
        text:      '\u2022',
        alignment: AlignmentType.LEFT,
        style: {
          paragraph: { indent: { left: 720, hanging: 360 } },
          run:       { color: BODY, size: 20, font: 'Arial' },
        },
      }],
    }],
  };

  const children = [
    ...buildCover(data),
    ...buildExecutiveSummary(data),
    ...buildFindingsSummary(data),
    ...buildMatchQuality(data),
    ...buildDataQuality(data),
    ...buildFieldChanges(data),
    ...buildReviewQueue(data),
    ...buildRejectedMatches(data),
    ...buildSanityGate(data),
    ...buildActionPlan(data),
  ];

  const doc = new Document({
    numbering,
    styles: {
      default: {
        document: { run: { font: 'Arial', size: 20, color: BODY } },
      },
    },
    sections: [{
      properties: {
        page: {
          size:   { width: PAGE_W, height: PAGE_H },
          margin: { top: MARGIN, right: MARGIN, bottom: MARGIN, left: MARGIN },
        },
      },
      footers:  { default: footer },
      children,
    }],
  });

  try {
    const buffer = await Packer.toBuffer(doc);
    fs.writeFileSync(args.out, buffer);
    console.log(`[build_report] saved: ${args.out}  (${(buffer.length / 1024).toFixed(0)} KB)`);
    process.exit(0);
  } catch (e) {
    console.error(`[build_report] Failed to write output: ${e.message}`);
    process.exit(1);
  }
}

main().catch((e) => {
  console.error('[build_report] Unexpected error:', e.message);
  process.exit(1);
});
