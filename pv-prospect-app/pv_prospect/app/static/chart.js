/**
 * chart.js — chart helpers (rendered in W1 and W2).
 * Requires uPlot to be loaded before this script.
 */

/**
 * Render a monthly bar chart for the prediction section (W1).
 * @param {HTMLElement} container
 * @param {string[]} months - ['Jan', ..., 'Dec']
 * @param {number[]} values_kwh - 12 values
 */
function renderBarChart(container, months, values_kwh) {
    // Implemented in W1.
}

/**
 * Render a time-series overlay for the validation section (W2).
 * @param {HTMLElement} container
 * @param {{ date: string, predicted_kwh: number, actual_kwh: number, clipped: boolean }[]} series
 */
function renderTimeSeriesChart(container, series) {
    // Implemented in W2.
}
