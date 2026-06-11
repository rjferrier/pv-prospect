/**
 * validation.js — controller for the Validation section (W2).
 *
 * Populates the site picker from GET /validate/sites, then on selection renders
 * the predicted-vs-actual overlay, headline metrics, and the endpoint's own
 * honest caveats from GET /validate/{system_id}. The picker shows only the
 * fields the contract carries (system_id + window dates) — there are no
 * client-side site names.
 *
 * Requires callApi (api.js) and renderTimeSeriesChart (chart.js).
 */
(function () {
    var select = document.getElementById('site-select');
    if (!select) { return; }
    var cfToggle = document.getElementById('cf-toggle');
    var statusEl = document.getElementById('validation-status');
    var resultEl = document.getElementById('validation-result');
    var metricsEl = document.getElementById('validation-metrics');
    var chartEl = document.getElementById('validation-chart');
    var caveatsEl = document.getElementById('validation-caveats');
    var metaEl = document.getElementById('validation-meta');

    var currentSeries = null;   // last-loaded series, for CF-toggle re-render
    var chartInstance = null;

    function showStatus(message, onRetry) {
        statusEl.textContent = message;
        if (onRetry) {
            var btn = document.createElement('button');
            btn.textContent = 'Retry';
            btn.className = 'retry-btn';
            btn.addEventListener('click', onRetry);
            statusEl.appendChild(document.createTextNode(' '));
            statusEl.appendChild(btn);
        }
        statusEl.hidden = false;
        resultEl.hidden = true;
    }

    function fmtMetric(value, kind) {
        if (value === null || value === undefined) { return 'n/a'; }
        if (kind === 'pct') { return (value * 100).toFixed(1) + '%'; }
        return value.toFixed(3);
    }

    function renderMetrics(error) {
        metricsEl.innerHTML = '';
        var stats = [
            { label: 'R² (power)', value: fmtMetric(error.power_space_r2, 'r2') },
            { label: 'MAPE', value: fmtMetric(error.mape, 'pct') },
        ];
        stats.forEach(function (s) {
            var box = document.createElement('div');
            box.className = 'stat';
            var v = document.createElement('span');
            v.className = 'stat-value';
            v.textContent = s.value;
            var l = document.createElement('span');
            l.className = 'stat-label';
            l.textContent = s.label;
            box.appendChild(v);
            box.appendChild(l);
            metricsEl.appendChild(box);
        });
    }

    function renderCaveats(caveats) {
        caveatsEl.innerHTML = '';
        caveats.forEach(function (c) {
            var li = document.createElement('li');
            li.textContent = c;
            caveatsEl.appendChild(li);
        });
    }

    function drawChart() {
        if (chartInstance) { chartInstance.destroy(); chartInstance = null; }
        var chartMode = cfToggle.checked ? 'cf' : 'kwh';
        chartInstance = renderTimeSeriesChart(chartEl, currentSeries, chartMode);
    }

    function renderSite(data) {
        currentSeries = data.series;
        renderMetrics(data.error);
        renderCaveats(data.caveats);
        metaEl.textContent =
            'Model ' + data.model_version +
            ' · window updated ' + data.window_updated_at;
        statusEl.hidden = true;
        resultEl.hidden = false;
        drawChart();
    }

    function loadSite(systemId) {
        showStatus('Loading site ' + systemId + '…');
        callApi('GET', '/validate/' + systemId).then(renderSite).catch(function (e) {
            if (e.type === 'warming') {
                showStatus('Validation data is warming up.',
                    function () { loadSite(systemId); });
            } else if (e.type === 'not_found') {
                showStatus('Site ' + systemId + ' is no longer in the window.');
            } else {
                showStatus('Could not load site ' + systemId + '.');
            }
        });
    }

    function populateSites(data) {
        select.innerHTML = '';
        if (!data.sites.length) {
            select.disabled = true;
            showStatus('No validation sites are currently available.');
            return;
        }
        data.sites.forEach(function (site) {
            var opt = document.createElement('option');
            opt.value = site.system_id;
            opt.textContent =
                'Site ' + site.system_id +
                ' (' + site.window_start + ' – ' + site.window_end + ')';
            select.appendChild(opt);
        });
        select.disabled = false;
        loadSite(data.sites[0].system_id);
    }

    function init() {
        showStatus('Loading validation sites…');
        callApi('GET', '/validate/sites').then(populateSites).catch(function (e) {
            if (e.type === 'warming') {
                showStatus('Validation data is warming up.', init);
            } else {
                showStatus('Could not load validation sites.');
            }
        });
    }

    select.addEventListener('change', function () {
        loadSite(parseInt(select.value, 10));
    });
    cfToggle.addEventListener('change', function () {
        if (currentSeries) { drawChart(); }
    });

    init();
})();
