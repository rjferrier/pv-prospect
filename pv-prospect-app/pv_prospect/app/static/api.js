/**
 * api.js — same-origin JSON API client with normalised error types.
 *
 * Errors thrown: { type, status, detail }
 * type values:
 *   'warming'      — 503: models not yet loaded
 *   'upstream'     — 502: elevation lookup or other upstream failure
 *   'not_found'    — 404: site not in registry or validation window
 *   'field_error'  — 422 with array detail: Pydantic field validation failure
 *   'domain_error' — 422 with string detail: outside-UK-domain rejection
 *   'rate_limited' — 429: per-IP rate limit exceeded
 *   'unknown'      — any other non-OK status
 */
async function callApi(method, path, body) {
    var opts = { method: method, headers: {} };
    if (body !== undefined) {
        opts.headers['Content-Type'] = 'application/json';
        opts.body = JSON.stringify(body);
    }
    var resp = await fetch(path, opts);
    var data;
    try { data = await resp.json(); } catch (_) { data = {}; }
    if (!resp.ok) {
        var detail = (data && data.detail !== undefined) ? data.detail : null;
        var type;
        if (resp.status === 503) {
            type = 'warming';
        } else if (resp.status === 502) {
            type = 'upstream';
        } else if (resp.status === 404) {
            type = 'not_found';
        } else if (resp.status === 422) {
            type = Array.isArray(detail) ? 'field_error' : 'domain_error';
        } else if (resp.status === 429) {
            type = 'rate_limited';
        } else {
            type = 'unknown';
        }
        throw { type: type, status: resp.status, detail: detail };
    }
    return data;
}
