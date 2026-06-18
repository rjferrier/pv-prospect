/**
 * validation.js — controller for the Validation section (W2).
 *
 * Populates the site picker from GET /validate/sites, then on selection renders
 * the predicted-vs-actual overlay, headline metrics, and the endpoint's own
 * honest caveats from GET /validate/{display_id}. Sites are also shown as
 * numbered markers on a UK map; clicking a marker selects the site. A unit
 * toggle switches the chart between kWh/day and kW avg / kW peak.
 *
 * Requires callApi (api.js), initValidationMap (map.js), and the chart helpers
 * (chart.js).
 */
(function () {
    var select = document.getElementById('site-select');
    if (!select) { return; }
    var segEl = document.getElementById('val-seg');
    var unitEl = document.getElementById('val-unit');
    var statusEl = document.getElementById('validation-status');
    var resultEl = document.getElementById('validation-result');
    var metricsEl = document.getElementById('validation-metrics');
    var chartEl = document.getElementById('validation-chart');
    var caveatsEl = document.getElementById('validation-caveats');
    var metaEl = document.getElementById('validation-meta');

    var currentSeries = null;     // last-loaded series, for re-render
    var chartMode = 'kwh';        // 'kwh' | 'cf'
    var currentDisplayId = null;  // display ID of the loaded site
    var valMapControl = null;     // {selectSite, invalidateSize} once initialised
    var pendingMapSites = null;   // stored until the map container is visible

    function showStatus(message, onRetry) {
        statusEl.textContent = message;
        if (onRetry) {
            var btn = document.createElement('button');
            btn.textContent = 'Retry';
            btn.className = 'retry-btn';
            btn.addEventListener('click', onRetry);
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
            var card = document.createElement('div');
            card.className = 'card statcard';
            var v = document.createElement('div');
            v.className = 'v';
            v.textContent = s.value;
            var l = document.createElement('div');
            l.className = 'l';
            l.textContent = s.label;
            card.appendChild(v);
            card.appendChild(l);
            metricsEl.appendChild(card);
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

    // ~8 evenly spaced x-axis ticks, labelled D/M (UK order) from the ISO dates.
    function dateTicks(series) {
        var n = series.length;
        var count = Math.min(8, n);
        var ticks = [];
        for (var k = 0; k < count; k++) {
            var idx = count === 1 ? 0 : Math.round(k * (n - 1) / (count - 1));
            var parts = series[idx].date.split('-');
            ticks.push({ idx: idx, text: Number(parts[2]) + '/' + Number(parts[1]) });
        }
        return ticks;
    }

    function drawChart() {
        if (!currentSeries) { return; }
        var cf = chartMode === 'cf';
        var actKey = cf ? 'actual_cf' : 'actual_kwh';
        var predKey = cf ? 'predicted_cf' : 'predicted_kwh';
        var actual = currentSeries.map(function (p) { return p[actKey]; });
        var predicted = currentSeries.map(function (p) { return p[predKey]; });
        var clippedIdx = [];
        currentSeries.forEach(function (p, i) { if (p.clipped) { clippedIdx.push(i); } });
        unitEl.textContent = cf ? 'kW avg / kW peak' : 'kWh / day';
        renderTimeSeriesChart(chartEl, dateTicks(currentSeries), actual, predicted, clippedIdx, {
            fmt: cf ? function (v) { return v.toFixed(2); } : function (v) { return Math.round(v); },
        });
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
            var page = document.getElementById('validation');
            if (page && page.classList.contains('active')) { ensureValMap(); }
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
    wireSegmentedToggle(segEl, function (mode) {
        chartMode = mode;
        drawChart();
    });

    // Leaflet measures its container on init; the validation page starts hidden,
    // so defer map init until the page is first shown.
    document.addEventListener('pv:pageshow', function (e) {
        if (e.detail.page === 'validation') { setTimeout(ensureValMap, 0); }
    });

    init();
})();
