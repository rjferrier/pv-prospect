/**
 * chart.js — dependency-free SVG chart drawing for the prediction (W1) and
 * validation (W2) sections. The charts are inline <svg> elements with a fixed
 * viewBox; CSS scales them responsively. Drawing functions are pure: callers
 * compute the per-mode data arrays and pass them in.
 */

var CHART_PALETTE = {
    blue: '#2b6fb0',
    navy: '#16395a',
    sun: '#f59e0b',
    grid: '#eef2f6',
    axis: '#9aa6b2',
    clip: '#c0344d',
};

/** Round a maximum up to a "nice" axis bound (1, 2, 2.5, 5, 10 × 10ⁿ). */
function niceMax(m) {
    if (m <= 0) { return 1; }
    var pow = Math.pow(10, Math.floor(Math.log10(m)));
    var n = m / pow;
    var f;
    if (n <= 1) { f = 1; }
    else if (n <= 2) { f = 2; }
    else if (n <= 2.5) { f = 2.5; }
    else if (n <= 5) { f = 5; }
    else { f = 10; }
    return f * pow;
}

/* --- small SVG element builders (numbers rounded to 1 dp for compactness) --- */
function _n(v) { return Number(v).toFixed(1); }
function _esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
function _line(x1, y1, x2, y2, stroke, w) {
    return '<line x1="' + _n(x1) + '" y1="' + _n(y1) + '" x2="' + _n(x2)
        + '" y2="' + _n(y2) + '" stroke="' + stroke + '" stroke-width="' + w + '"/>';
}
function _text(x, y, s, anchor, fill, size) {
    return '<text x="' + _n(x) + '" y="' + _n(y) + '" text-anchor="' + anchor
        + '" font-family="var(--mono)" font-size="' + size + '" fill="' + fill + '">'
        + _esc(s) + '</text>';
}
function _noData(svg, msg) {
    svg.innerHTML = _text(450, 150, msg || 'No data.', 'middle', CHART_PALETTE.axis, 14);
}

/**
 * Render a monthly bar chart with optional symmetric error whiskers.
 * @param {SVGElement} svg     - target inline <svg> (viewBox 0 0 900 300)
 * @param {string[]}   labels  - one x-axis label per bar
 * @param {number[]}   values  - one value per bar
 * @param {{fmt?:function, errFrac?:number}} [opts]
 *        fmt formats axis tick values; errFrac (e.g. 0.17) draws ±errFrac whiskers.
 */
function renderBarChart(svg, labels, values, opts) {
    if (!values || !values.length) { _noData(svg); return; }
    opts = opts || {};
    var fmt = opts.fmt || function (v) { return Math.round(v); };
    var err = opts.errFrac || 0;
    var W = 900, R = 16, L = 56, T = 12, B = 38, pw = W - L - R, ph = 300 - T - B;
    var ymax = niceMax(Math.max.apply(null, values) * (1 + err));
    var Y = function (v) { return T + ph * (1 - v / ymax); };
    var n = values.length, gap = pw / n, bw = gap * 0.58;
    var g = '';
    for (var k = 0; k <= 5; k++) {
        var v = ymax * k / 5, y = Y(v);
        g += _line(L, y, W - R, y, CHART_PALETTE.grid, 1);
        g += _text(L - 9, y + 3, fmt(v), 'end', CHART_PALETTE.axis, 11);
    }
    values.forEach(function (val, i) {
        var cx = L + gap * i + gap / 2, x = cx - bw / 2, y = Y(val), h = T + ph - y;
        g += '<rect x="' + _n(x) + '" y="' + _n(y) + '" width="' + _n(bw)
            + '" height="' + _n(h) + '" rx="3" fill="' + CHART_PALETTE.blue + '" opacity="0.9"/>';
        if (err > 0) {
            var hi = Y(val * (1 + err)), lo = Y(Math.max(0, val * (1 - err)));
            g += _line(cx, hi, cx, lo, CHART_PALETTE.navy, 1.6);
            g += _line(cx - 4, hi, cx + 4, hi, CHART_PALETTE.navy, 1.6);
            g += _line(cx - 4, lo, cx + 4, lo, CHART_PALETTE.navy, 1.6);
        }
        g += _text(cx, 300 - 12, labels[i] || '', 'middle', CHART_PALETTE.axis, 11);
    });
    svg.innerHTML = g;
}

/**
 * Render an actual-vs-predicted daily time series with clipped-day markers.
 * @param {SVGElement} svg       - target inline <svg> (viewBox 0 0 900 320)
 * @param {{idx:number,text:string}[]} ticks - x-axis tick positions/labels
 * @param {number[]} actual
 * @param {number[]} predicted
 * @param {number[]} clippedIdx  - indices of clipped (inverter-limited) days
 * @param {{fmt?:function}} [opts]
 */
function renderTimeSeriesChart(svg, ticks, actual, predicted, clippedIdx, opts) {
    if (!actual || !actual.length) { _noData(svg, 'No data for this site.'); return; }
    opts = opts || {};
    var fmt = opts.fmt || function (v) { return Math.round(v); };
    var W = 900, R = 18, L = 50, T = 14, B = 42, pw = W - L - R, ph = 320 - T - B;
    var n = actual.length;
    var peak = Math.max(Math.max.apply(null, actual), Math.max.apply(null, predicted));
    var ymax = niceMax(peak * 1.05);
    var X = function (i) { return L + pw * i / (n > 1 ? n - 1 : 1); };
    var Y = function (v) { return T + ph * (1 - v / ymax); };
    var g = '';
    for (var k = 0; k <= 5; k++) {
        var v = ymax * k / 5, y = Y(v);
        g += _line(L, y, W - R, y, CHART_PALETTE.grid, 1);
        g += _text(L - 8, y + 3, fmt(v), 'end', CHART_PALETTE.axis, 11);
    }
    (ticks || []).forEach(function (t) {
        g += _text(X(t.idx), 320 - 16, t.text, 'middle', CHART_PALETTE.axis, 11);
    });
    var poly = function (arr) {
        return arr.map(function (v, i) { return _n(X(i)) + ',' + _n(Y(v)); }).join(' ');
    };
    g += '<polyline points="' + poly(predicted) + '" fill="none" stroke="'
        + CHART_PALETTE.sun + '" stroke-width="2.4" stroke-linejoin="round"/>';
    g += '<polyline points="' + poly(actual) + '" fill="none" stroke="'
        + CHART_PALETTE.blue + '" stroke-width="2.4" stroke-linejoin="round"/>';
    (clippedIdx || []).forEach(function (i) {
        g += '<rect x="' + _n(X(i) - 4) + '" y="' + _n(Y(actual[i]) - 4)
            + '" width="8" height="8" fill="none" stroke="' + CHART_PALETTE.clip
            + '" stroke-width="2"/>';
    });
    svg.innerHTML = g;
}

/** Wire a segmented toggle (.seg) so each button sets `active` and reports its mode. */
function wireSegmentedToggle(segEl, onChange) {
    if (!segEl) { return; }
    segEl.querySelectorAll('button').forEach(function (btn) {
        btn.addEventListener('click', function () {
            segEl.querySelectorAll('button').forEach(function (b) { b.classList.remove('active'); });
            btn.classList.add('active');
            onChange(btn.dataset.mode);
        });
    });
}
