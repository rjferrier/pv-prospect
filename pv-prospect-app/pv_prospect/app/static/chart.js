/**
 * chart.js — chart helpers (rendered in W1 and W2).
 * Requires uPlot to be loaded before this script.
 */

/**
 * Render a monthly bar chart for the prediction section (W1).
 * @param {HTMLElement} container
 * @param {string[]} months - e.g. ['Jan', ..., 'Dec']
 * @param {number[]} values_kwh - one value per month
 * @returns {uPlot|null} the chart instance, or null when there is no data.
 */
function renderBarChart(container, months, values_kwh) {
    container.innerHTML = '';
    if (!values_kwh || !values_kwh.length) {
        container.textContent = 'No data.';
        return null;
    }
    var xs = months.map(function (_, i) { return i; });
    var opts = {
        width: container.clientWidth || 720,
        height: 320,
        scales: { x: { time: false, range: [-0.6, months.length - 0.4] } },
        axes: [
            {
                splits: function () { return xs; },
                values: function (u, splits) {
                    return splits.map(function (i) { return months[i] || ''; });
                },
            },
            { label: 'kWh / month' },
        ],
        series: [
            {},
            {
                label: 'Energy',
                stroke: '#1f6fd6',
                fill: 'rgba(31, 111, 214, 0.45)',
                paths: uPlot.paths.bars({ size: [0.7, 60] }),
                points: { show: false },
            },
        ],
    };
    return new uPlot(opts, [xs, values_kwh], container);
}

/**
 * Render a predicted-vs-actual time-series overlay for the validation section.
 * Actual and predicted are drawn as lines; clipped (inverter-limited) days are
 * marked with distinct points (those days are excluded from the error metrics).
 *
 * @param {HTMLElement} container
 * @param {{ date: string, predicted_kwh: number, actual_kwh: number,
 *           predicted_cf: number, actual_cf: number, clipped: boolean }[]} series
 * @param {'kwh'|'cf'} [mode] - 'kwh' (default) plots energy; 'cf' plots the
 *        raw capacity factor (predicted_cf is pre-clamp, so on clipped days it
 *        can exceed what predicted_kwh reflects).
 * @returns {uPlot|null} the chart instance, or null when there is no data.
 */
function renderTimeSeriesChart(container, series, mode) {
    mode = mode || 'kwh';
    container.innerHTML = '';
    if (!series || !series.length) {
        container.textContent = 'No data for this site.';
        return null;
    }

    var predKey = mode === 'cf' ? 'predicted_cf' : 'predicted_kwh';
    var actKey = mode === 'cf' ? 'actual_cf' : 'actual_kwh';
    var xs = series.map(function (p) { return Date.parse(p.date) / 1000; });
    var actual = series.map(function (p) { return p[actKey]; });
    var predicted = series.map(function (p) { return p[predKey]; });
    var clippedMark = series.map(function (p) {
        return p.clipped ? p[actKey] : null;
    });

    var opts = {
        width: container.clientWidth || 720,
        height: 360,
        scales: { x: { time: true } },
        series: [
            {},
            { label: 'Actual', stroke: '#1f6fd6', width: 2 },
            { label: 'Predicted', stroke: '#f5b829', width: 2 },
            {
                label: 'Clipped (inverter-limited)',
                stroke: '#b00020',
                width: 0,
                points: { show: true, size: 9 },
            },
        ],
        axes: [
            {},
            { label: mode === 'cf' ? 'Capacity factor' : 'kWh / day' },
        ],
    };
    return new uPlot(opts, [xs, actual, predicted, clippedMark], container);
}
