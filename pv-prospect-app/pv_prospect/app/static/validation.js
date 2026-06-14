/**
 * validation.js — controller for the Validation section.
 *
 * Populates the site picker from GET /validate/sites, then on selection renders
 * the predicted-vs-actual overlay, headline metrics, and the endpoint's own
 * honest caveats from GET /validate/{display_id}.  Sites are also shown as
 * numbered markers on a UK map; clicking a marker selects the site.
 *
 * Requires callApi (api.js), initValidationMap (map.js), and renderTimeSeriesChart (chart.js).
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
    var currentDisplayId = null;  // display ID of the loaded site
    var valMapControl = null;     // {selectSite, invalidateSize} once initialised
    var pendingMapSites = null;   // stored until the tab is first made visible

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
        currentDisplayId = data.system_id;
        renderMetrics(data.error);
        renderCaveats(data.caveats);
        metaEl.textContent =
            'Model ' + data.model_version +
            ' · window updated ' + data.window_updated_at;
        statusEl.hidden = true;
        resultEl.hidden = false;
        drawChart();
        if (valMapControl) { valMapControl.selectSite(data.system_id); }
    }

    function loadSite(displayId) {
        showStatus('Loading site ' + displayId + '…');
        callApi('GET', '/validate/' + displayId).then(renderSite).catch(function (e) {
            if (e.type === 'warming') {
                showStatus('Validation data is warming up.',
                    function () { loadSite(displayId); });
            } else if (e.type === 'not_found') {
                showStatus('Site ' + displayId + ' is no longer in the window.');
            } else if (e.type === 'rate_limited') {
                showStatus('You\'re sending requests too quickly. Wait a moment and try again.',
                    function () { loadSite(displayId); });
            } else {
                showStatus('Could not load site ' + displayId + '.');
            }
        });
    }

    /* Initialise the validation map on the now-visible container.
     * Safe to call multiple times — subsequent calls just invalidate the size.
     * No-op if site coordinates have not yet arrived from the API. */
    function ensureValMap() {
        if (valMapControl) { valMapControl.invalidateSize(); return; }
        if (!pendingMapSites) { return; }
        valMapControl = initValidationMap('validation-map', pendingMapSites, function (displayId) {
            select.value = displayId;
            loadSite(displayId);
        });
        if (currentDisplayId !== null) {
            valMapControl.selectSite(currentDisplayId);
        }
    }

    function populateSites(data) {
        select.innerHTML = '';
        if (!data.sites.length) {
            select.disabled = true;
            showStatus('No validation sites are currently available.');
            return;
        }
        var mapSites = [];
        data.sites.forEach(function (site) {
            var opt = document.createElement('option');
            opt.value = site.system_id;
            opt.textContent =
                'Site ' + site.system_id +
                ' (' + site.window_start + ' – ' + site.window_end + ')';
            select.appendChild(opt);
            if (site.latitude != null && site.longitude != null) {
                mapSites.push({ display_id: site.system_id, lat: site.latitude, lng: site.longitude });
            }
        });
        select.disabled = false;
        if (mapSites.length) {
            pendingMapSites = mapSites;
            var panel = document.getElementById('panel-validation');
            if (panel && !panel.hidden) { ensureValMap(); }
        }
        loadSite(data.sites[0].system_id);
    }

    function init() {
        showStatus('Loading validation sites…');
        callApi('GET', '/validate/sites').then(populateSites).catch(function (e) {
            if (e.type === 'warming') {
                showStatus('Validation data is warming up.', init);
            } else if (e.type === 'rate_limited') {
                showStatus('You\'re sending requests too quickly. Wait a moment and try again.', init);
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

    /* Leaflet measures its container on init; the validation tab starts hidden,
     * so defer map init until the tab is first shown. */
    var valTabBtn = document.getElementById('tab-validation');
    if (valTabBtn) {
        valTabBtn.addEventListener('click', function () { setTimeout(ensureValMap, 0); });
    }

    init();
})();
