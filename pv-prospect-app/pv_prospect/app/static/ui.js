/**
 * ui.js — chrome shared across pages: brand-mark injection, hash routing,
 * the illustrative hero UK map, and the footer model-version line.
 *
 * Routing dispatches a `pv:pageshow` CustomEvent ({detail:{page}}) so the
 * prediction/validation controllers can (re)measure their Leaflet maps when
 * their page first becomes visible. Requires callApi (api.js).
 */
(function () {
    var PAGES = ['home', 'prediction', 'validation', 'about'];

    // ---- inject the reusable logo mark into every .mark slot ----
    var markTmpl = document.getElementById('mark-tmpl');
    document.querySelectorAll('.mark').forEach(function (slot) {
        slot.appendChild(markTmpl.content.cloneNode(true));
    });

    // ---- hash routing ----
    var navlinks = document.querySelectorAll('.navlinks a[data-page]');

    function show(id) {
        if (PAGES.indexOf(id) === -1) { id = 'home'; }
        document.querySelectorAll('.page').forEach(function (p) {
            p.classList.toggle('active', p.id === id);
        });
        navlinks.forEach(function (a) {
            a.classList.toggle('active', a.dataset.page === id);
        });
        if (location.hash.slice(1) !== id) { history.replaceState(null, '', '#' + id); }
        window.scrollTo(0, 0);
        document.dispatchEvent(new CustomEvent('pv:pageshow', { detail: { page: id } }));
    }

    document.querySelectorAll('[data-page]').forEach(function (el) {
        el.addEventListener('click', function (e) {
            e.preventDefault();
            show(el.dataset.page);
        });
    });
    window.addEventListener('hashchange', function () { show(location.hash.slice(1)); });

    show(location.hash.slice(1) || 'home');

    // ---- illustrative hero UK map (smoothed coastlines + PV-potential bands) ----
    (function buildHeroMap() {
        function spline(pts, closed) {
            var n = pts.length;
            var get = function (i) {
                return closed ? pts[((i % n) + n) % n] : pts[Math.max(0, Math.min(n - 1, i))];
            };
            var d = 'M ' + pts[0][0] + ' ' + pts[0][1];
            var segs = closed ? n : n - 1;
            for (var i = 0; i < segs; i++) {
                var p0 = get(i - 1), p1 = get(i), p2 = get(i + 1), p3 = get(i + 2);
                var c1x = p1[0] + (p2[0] - p0[0]) / 6, c1y = p1[1] + (p2[1] - p0[1]) / 6;
                var c2x = p2[0] - (p3[0] - p1[0]) / 6, c2y = p2[1] - (p3[1] - p1[1]) / 6;
                d += ' C ' + c1x.toFixed(1) + ' ' + c1y.toFixed(1) + ', '
                    + c2x.toFixed(1) + ' ' + c2y.toFixed(1) + ', ' + p2[0] + ' ' + p2[1];
            }
            if (closed) { d += ' Z'; }
            return d;
        }
        var gbPath = document.getElementById('gbpath');
        if (!gbPath) { return; }
        var GB = [[110, 14], [126, 20], [150, 42], [140, 58], [160, 78], [150, 96], [168, 128],
                  [160, 150], [186, 182], [172, 196], [180, 214], [158, 220], [150, 230],
                  [120, 224], [92, 238], [86, 228], [110, 220], [100, 206], [120, 212], [96, 200],
                  [104, 182], [88, 176], [112, 164], [104, 150], [120, 138], [100, 128], [112, 112],
                  [94, 96], [110, 80], [92, 64], [108, 46], [96, 32]];
        var IRE = [[70, 178], [84, 184], [83, 206], [72, 226], [52, 230], [40, 214], [44, 192], [56, 179]];
        var C = [
            [[88, 116], [120, 108], [150, 118], [182, 126]],
            [[92, 152], [125, 144], [155, 152], [186, 160]],
            [[96, 188], [128, 180], [158, 186], [186, 192]],
            [[100, 216], [130, 210], [158, 216], [180, 218]],
        ];
        var gb = spline(GB, true);
        gbPath.setAttribute('d', gb);
        document.getElementById('gboutline').setAttribute('d', gb);
        document.getElementById('ireland').setAttribute('d', spline(IRE, true));
        ['c1', 'c2', 'c3', 'c4'].forEach(function (id, i) {
            document.getElementById(id).setAttribute('d', spline(C[i], false));
        });
    })();

    // ---- footer: model version from /version ----
    var versionEl = document.getElementById('footer-version');
    callApi('GET', '/version').then(function (v) {
        var parts = [
            'PV model: ' + v.pv_model_version,
            'R²=' + v.pv_critical_metric_r2.toFixed(3),
            'Weather: ' + v.weather_model_version,
        ];
        if (v.window_loaded && v.window_updated_at) {
            parts.push('Window: ' + v.window_updated_at);
        }
        versionEl.textContent = parts.join(' · ');
    }).catch(function (e) {
        versionEl.textContent = e.type === 'warming' ? 'Models warming up…' : '';
    });
})();
