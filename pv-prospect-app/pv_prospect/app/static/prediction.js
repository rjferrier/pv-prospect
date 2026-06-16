/**
 * prediction.js — controller for the Prediction section (W1).
 *
 * A UK map click sets the location; the form supplies the panel parameters; a
 * POST /predict returns the climatological annual yield. The result card shows
 * the expected estimate, its uncertainty range (uncertainty.annual_kwh_low /
 * _high, the LOSO-calibrated 1σ floor), and a monthly chart with a unit toggle
 * (kWh/month · kWh/day · kW avg / kW peak). The endpoint's caveats render
 * verbatim.
 *
 * Requires callApi (api.js), initMap (map.js), and the chart helpers (chart.js).
 */
(function () {
    var form = document.getElementById('prediction-form');
    if (!form) { return; }

    var locEl = document.getElementById('prediction-location');
    var latlngEl = document.getElementById('prediction-latlng');
    var pindotEl = locEl.querySelector('.pindot');
    var submitBtn = document.getElementById('pred-submit');
    var statusEl = document.getElementById('prediction-status');
    var resultEl = document.getElementById('prediction-result');
    var valueEl = document.getElementById('pred-value');
    var rangeEl = document.getElementById('pred-range');
    var unitEl = document.getElementById('pred-unit');
    var segEl = document.getElementById('pred-seg');
    var chartEl = document.getElementById('prediction-chart');
    var caveatsEl = document.getElementById('prediction-caveats');
    var metaEl = document.getElementById('prediction-meta');

    var MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    // Representative typical year: the last complete calendar year. The model is
    // climatological, so the specific year only frames the request period.
    var YEAR = new Date().getFullYear() - 1;
    var DAYS = MONTHS.map(function (_, i) { return new Date(YEAR, i + 1, 0).getDate(); });

    var selected = null;       // { lat, lng } once the user clicks the map
    var map = null;
    var current = null;        // last /predict response, for chart re-render
    var chartMode = 'month';

    // Leaflet measures its container on init; the prediction page starts hidden,
    // so defer init until the page is first shown (and invalidate thereafter).
    function ensureMap() {
        if (map) { map.invalidateSize(); return; }
        map = initMap('prediction-map', function (lat, lng) {
            selected = { lat: lat, lng: lng };
            latlngEl.textContent = lat.toFixed(4) + ', ' + lng.toFixed(4);
            latlngEl.classList.remove('ph');
            locEl.classList.add('chosen');
            if (pindotEl) { pindotEl.hidden = false; }
            submitBtn.disabled = false;
        });
    }
    document.addEventListener('pv:pageshow', function (e) {
        if (e.detail.page === 'prediction') { setTimeout(ensureMap, 0); }
    });

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

    function kwh(value) { return Math.round(value).toLocaleString(); }

    // Build per-mode bar values + formatting from the stored response.
    function modeData(mode) {
        var monthly = current.monthly_kwh;
        var peakKw = (readNumber('pred-capacity') || 0) / 1000;
        if (mode === 'day') {
            return {
                values: monthly.map(function (v, i) { return v / DAYS[i]; }),
                unit: 'kWh / day',
                fmt: function (v) { return v.toFixed(1); },
            };
        }
        if (mode === 'cf') {
            return {
                values: monthly.map(function (v, i) {
                    return peakKw > 0 ? (v / (DAYS[i] * 24)) / peakKw : 0;
                }),
                unit: 'kW avg / kW peak',
                fmt: function (v) { return v.toFixed(2); },
            };
        }
        return {
            values: monthly.slice(),
            unit: 'kWh / month',
            fmt: function (v) { return Math.round(v); },
        };
    }

    function drawChart() {
        if (!current) { return; }
        var d = modeData(chartMode);
        unitEl.textContent = d.unit;
        renderBarChart(chartEl, MONTHS, d.values, {
            fmt: d.fmt,
            errFrac: current.uncertainty.sigma_frac,
        });
    }

    function renderPrediction(data) {
        current = data;
        var band = data.uncertainty;
        var pct = (band.sigma_frac * 100).toFixed(0);
        valueEl.textContent = kwh(data.expected_annual_kwh);
        rangeEl.innerHTML = 'Range <b>' + kwh(band.annual_kwh_low) + ' – '
            + kwh(band.annual_kwh_high) + '</b> kWh/year'
            + '<span class="pm">±' + pct + '%</span>';

        renderCaveats(data.caveats);
        var a = data.assumptions;
        metaEl.textContent = 'Elevation ' + Math.round(a.elevation_m) + ' m'
            + ' · PV model ' + a.pv_model_version
            + ' · weather ' + a.weather_model_version
            + ' · ' + YEAR;

        statusEl.hidden = true;
        resultEl.hidden = false;
        drawChart();
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

    wireSegmentedToggle(segEl, function (mode) {
        chartMode = mode;
        drawChart();
    });
})();
