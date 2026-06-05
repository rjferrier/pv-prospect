/**
 * map.js — Leaflet map helper, clamped to UK bounds.
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
