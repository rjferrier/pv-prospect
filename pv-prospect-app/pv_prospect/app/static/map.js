/**
 * map.js — Leaflet map helpers, clamped to UK bounds.
 * Requires Leaflet to be loaded before this script.
 */
var UK_BOUNDS = [[49.5, -9.0], [61.0, 2.0]];

/**
 * Initialise a Leaflet map inside elementId.
 * onLatLng(lat, lng) is called whenever the user clicks to place a pin.
 * Returns the Leaflet map instance.
 */
function initMap(elementId, onLatLng) {
    var map = L.map(elementId, {
        maxBounds: L.latLngBounds(UK_BOUNDS),
        maxBoundsViscosity: 1.0,
        minZoom: 4,
    }).setView([54.0, -2.0], 6);

    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution:
            '&copy; <a href="https://www.openstreetmap.org/copyright">'
            + 'OpenStreetMap</a> contributors',
        maxZoom: 18,
    }).addTo(map);

    var marker = null;
    map.on('click', function(e) {
        var lat = e.latlng.lat;
        var lng = e.latlng.lng;
        if (marker) { marker.remove(); }
        marker = L.marker([lat, lng]).addTo(map);
        if (onLatLng) { onLatLng(lat, lng); }
    });

    return map;
}

/**
 * Initialise a Leaflet map for validation site selection.
 * sites: array of {display_id, lat, lng}.
 * onSiteSelected(display_id) is called when a marker is clicked.
 * Returns {selectSite(displayId), invalidateSize()} — call invalidateSize()
 * after the containing element becomes visible.
 */
function initValidationMap(elementId, sites, onSiteSelected) {
    var map = L.map(elementId, {
        maxBounds: L.latLngBounds(UK_BOUNDS),
        maxBoundsViscosity: 1.0,
        minZoom: 4,
    }).setView([54.0, -2.0], 6);

    L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution:
            '&copy; <a href="https://www.openstreetmap.org/copyright">'
            + 'OpenStreetMap</a> contributors',
        maxZoom: 18,
    }).addTo(map);

    var markers = {};
    sites.forEach(function (site) {
        var icon = L.divIcon({
            html: String(site.display_id),
            className: 'site-marker',
            iconSize: [24, 24],
            iconAnchor: [12, 12],
        });
        var marker = L.marker([site.lat, site.lng], { icon: icon }).addTo(map);
        marker.on('click', function () { onSiteSelected(site.display_id); });
        markers[site.display_id] = marker;
    });

    var selectedId = null;

    function selectSite(displayId) {
        if (selectedId !== null && markers[selectedId]) {
            var prev = markers[selectedId].getElement();
            if (prev) { prev.classList.remove('site-marker--active'); }
        }
        selectedId = displayId;
        if (markers[displayId]) {
            var el = markers[displayId].getElement();
            if (el) { el.classList.add('site-marker--active'); }
        }
    }

    return {
        selectSite: selectSite,
        invalidateSize: function () { map.invalidateSize(); },
    };
}
