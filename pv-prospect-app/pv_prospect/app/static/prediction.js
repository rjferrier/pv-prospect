/**
 * prediction.js — controller for the Prediction section (W1).
 *
 * A UK map click sets the location; the form supplies the panel parameters; a
 * POST /predict returns the climatological annual yield. The headline renders
 * the expected estimate plus its uncertainty band (uncertainty.annual_kwh_low /
 * _high, the LOSO-calibrated 1σ floor), the monthly bars render monthly_kwh, and
 * the endpoint's own caveats are rendered verbatim.
 *
 * Requires callApi (api.js), initMap (map.js) and renderBarChart (chart.js).
 */
(function () {
    var form = document.getElementById('prediction-form');
    if (!form) { return; }

    var latlngEl = document.getElementById('prediction-latlng');
    var submitBtn = document.getElementById('pred-submit');
    var statusEl = document.getElementById('prediction-status');
    var resultEl = document.getElementById('prediction-result');
    var headlineEl = document.getElementById('prediction-headline');
    var chartEl = document.getElementById('prediction-chart');
    var caveatsEl = document.getElementById('prediction-caveats');
    var metaEl = document.getElementById('prediction-meta');

    var MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    // Representative typical year: the last complete calendar year. The model is
    // climatological, so the specific year only frames the request period.
    var YEAR = new Date().getFullYear() - 1;

    var selected = null;       // { lat, lng } once the user clicks the map
    var map = null;
    var chartInstance = null;

    // Leaflet measures its container on init; the prediction tab starts hidden,
    // so defer init until the tab is first shown (and invalidate thereafter).
    function ensureMap() {
        if (map) { map.invalidateSize(); return; }
        map = initMap('prediction-map', function (lat, lng) {
            selected = { lat: lat, lng: lng };
            latlngEl.textContent = lat.toFixed(4) + ', ' + lng.toFixed(4);
            submitBtn.disabled = false;
        });
    }
    var tabBtn = document.getElementById('tab-prediction');
    if (tabBtn) {
        // The inline tab handler un-hides the panel on the same click; defer so
        // Leaflet measures the now-visible container.
        tabBtn.addEventListener('click', function () { setTimeout(ensureMap, 0); });
    }
    // Prediction is the default-visible tab, so also initialise the map on load.
    setTimeout(ensureMap, 0);

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

    function readNumber(id) {
        var el = document.getElementById(id);
        return el.value === '' ? null : Number(el.value);
    }

    function renderCaveats(caveats) {
        caveatsEl.innerHTML = '';
        caveats.forEach(function (c) {
            var li = document.createElement('li');
            li.textContent = c;
            caveatsEl.appendChild(li);
        });
    }

    function kwh(value) {
        return Math.round(value).toLocaleString() + ' kWh/year';
    }

    function renderPrediction(data) {
        var band = data.uncertainty;
        var pct = (band.sigma_frac * 100).toFixed(0);
        headlineEl.innerHTML = '';
        var value = document.createElement('div');
        value.className = 'headline-value';
        value.textContent = kwh(data.expected_annual_kwh);
        var range = document.createElement('div');
        range.className = 'headline-range';
        range.textContent = '±' + pct + ' % (1σ floor): '
            + kwh(band.annual_kwh_low) + ' – ' + kwh(band.annual_kwh_high);
        headlineEl.appendChild(value);
        headlineEl.appendChild(range);

        renderCaveats(data.caveats);
        var a = data.assumptions;
        metaEl.textContent = 'Elevation ' + Math.round(a.elevation_m) + ' m'
            + ' · PV model ' + a.pv_model_version
            + ' · weather ' + a.weather_model_version
            + ' · ' + YEAR;

        statusEl.hidden = true;
        resultEl.hidden = false;
        if (chartInstance) { chartInstance.destroy(); chartInstance = null; }
        chartInstance = renderBarChart(chartEl, MONTHS, data.monthly_kwh);
    }

    function handleError(e) {
        if (e.type === 'warming') {
            showStatus('The model is warming up. Try again in a moment.', submit);
        } else if (e.type === 'upstream') {
            showStatus('Elevation lookup failed (transient). Try again.', submit);
        } else if (e.type === 'rate_limited') {
            showStatus('You\'re sending requests too quickly. Wait a moment and try again.', submit);
        } else if (e.type === 'domain_error') {
            showStatus(typeof e.detail === 'string' ? e.detail
                : 'That point is inside the bounding box but outside the UK '
                + 'modelling domain. Pick a point on land in Great Britain.');
        } else if (e.type === 'field_error') {
            var msg = 'Please check the form values.';
            if (Array.isArray(e.detail) && e.detail.length) {
                msg = e.detail.map(function (d) {
                    var loc = d.loc || [];
                    return loc[loc.length - 1] + ': ' + d.msg;
                }).join('; ');
            }
            showStatus(msg);
        } else {
            showStatus('Could not produce an estimate.');
        }
    }

    function submit() {
        if (!selected) { return; }
        var body = {
            latitude: selected.lat,
            longitude: selected.lng,
            start_date: YEAR + '-01-01',
            end_date: YEAR + '-12-31',
            panels_capacity_w: readNumber('pred-capacity'),
            azimuth_deg: readNumber('pred-azimuth'),
            tilt_deg: readNumber('pred-tilt'),
            install_age_years: readNumber('pred-age') || 0,
        };
        showStatus('Estimating…');
        callApi('POST', '/predict', body).then(renderPrediction).catch(handleError);
    }

    form.addEventListener('submit', function (e) {
        e.preventDefault();
        submit();
    });
})();
