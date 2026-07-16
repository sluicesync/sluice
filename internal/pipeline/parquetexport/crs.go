// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package parquetexport

import "encoding/json"

// Bundled PROJJSON CRS definitions for the GeoParquet "geo" block's
// per-column `crs` field (audit MED-D0-4). GeoParquet 1.1 accepts
// exactly two shapes there: a PROJJSON object (validated against
// https://proj.org/schemas/v0.7/projjson.schema.json — a bare
// authority:code reference is NOT schema-valid PROJJSON) or an explicit
// null meaning "CRS undefined/unknown". An OMITTED crs key is a third,
// different thing: the spec defaults it to OGC:CRS84 lon/lat degrees —
// which is precisely why the exporter must never omit it for a
// projected column (a geometry(Point,3857) exported without crs reads
// back as degrees, silently).
//
// The documents below are the canonical EPSG registry PROJJSON,
// embedded VERBATIM (via spatialreference.org / PROJ, 2026-07-15)
// rather than hand-minimized — a self-trimmed CRS document is the
// fixture-blindness trap: it validates against what we trimmed it to,
// not against what readers expect. Readers that only key off `id`
// (the spec sanctions this for CRS84-equivalence checks) find
// `id.authority`/`id.code` at the top level either way.
//
// The set is deliberately tiny: EPSG:4326 (the overwhelmingly common
// geographic SRID; also the GeoParquet default's functional twin) and
// EPSG:3857 (web-mercator, the common PROJECTED SRID — the one the
// omitted-crs default silently misreads as degrees). Any other SRID is
// stamped `null` + an operator-visible note; growing this map is cheap
// when a real corpus demands it.
var bundledCRS = map[int]json.RawMessage{
	4326: json.RawMessage(epsg4326PROJJSON),
	3857: json.RawMessage(epsg3857PROJJSON),
}

// epsg4326PROJJSON is the canonical PROJJSON for EPSG:4326 (WGS 84).
// Note the EPSG-official axis order is Lat,Lon — per the GeoParquet
// spec that does NOT describe the WKB byte order: "the axis order of
// the coordinates in WKB stored in a GeoParquet ... is therefore
// always (x, y)", i.e. lon/lat, which is exactly what the engines'
// WKB carries.
const epsg4326PROJJSON = `{"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"WGS 84","datum_ensemble":{"name":"World Geodetic System 1984 ensemble","members":[{"name":"World Geodetic System 1984 (Transit)","id":{"authority":"EPSG","code":1166}},{"name":"World Geodetic System 1984 (G730)","id":{"authority":"EPSG","code":1152}},{"name":"World Geodetic System 1984 (G873)","id":{"authority":"EPSG","code":1153}},{"name":"World Geodetic System 1984 (G1150)","id":{"authority":"EPSG","code":1154}},{"name":"World Geodetic System 1984 (G1674)","id":{"authority":"EPSG","code":1155}},{"name":"World Geodetic System 1984 (G1762)","id":{"authority":"EPSG","code":1156}},{"name":"World Geodetic System 1984 (G2139)","id":{"authority":"EPSG","code":1309}},{"name":"World Geodetic System 1984 (G2296)","id":{"authority":"EPSG","code":1383}}],"ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563},"accuracy":"2.0","id":{"authority":"EPSG","code":6326}},"coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"},{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"}]},"scope":"Horizontal component of 3D system.","area":"World.","bbox":{"south_latitude":-90,"west_longitude":-180,"north_latitude":90,"east_longitude":180},"id":{"authority":"EPSG","code":4326}}`

// epsg3857PROJJSON is the canonical PROJJSON for EPSG:3857
// (WGS 84 / Pseudo-Mercator) — projected metres, the flagship case the
// crs stamp exists for.
const epsg3857PROJJSON = `{"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"ProjectedCRS","name":"WGS 84 / Pseudo-Mercator","base_crs":{"type":"GeographicCRS","name":"WGS 84","datum_ensemble":{"name":"World Geodetic System 1984 ensemble","members":[{"name":"World Geodetic System 1984 (Transit)","id":{"authority":"EPSG","code":1166}},{"name":"World Geodetic System 1984 (G730)","id":{"authority":"EPSG","code":1152}},{"name":"World Geodetic System 1984 (G873)","id":{"authority":"EPSG","code":1153}},{"name":"World Geodetic System 1984 (G1150)","id":{"authority":"EPSG","code":1154}},{"name":"World Geodetic System 1984 (G1674)","id":{"authority":"EPSG","code":1155}},{"name":"World Geodetic System 1984 (G1762)","id":{"authority":"EPSG","code":1156}},{"name":"World Geodetic System 1984 (G2139)","id":{"authority":"EPSG","code":1309}},{"name":"World Geodetic System 1984 (G2296)","id":{"authority":"EPSG","code":1383}}],"ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563},"accuracy":"2.0","id":{"authority":"EPSG","code":6326}},"coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"},{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"}]},"id":{"authority":"EPSG","code":4326}},"conversion":{"name":"Popular Visualisation Pseudo-Mercator","method":{"name":"Popular Visualisation Pseudo Mercator","id":{"authority":"EPSG","code":1024}},"parameters":[{"name":"Latitude of natural origin","value":0,"unit":"degree","id":{"authority":"EPSG","code":8801}},{"name":"Longitude of natural origin","value":0,"unit":"degree","id":{"authority":"EPSG","code":8802}},{"name":"False easting","value":0,"unit":"metre","id":{"authority":"EPSG","code":8806}},{"name":"False northing","value":0,"unit":"metre","id":{"authority":"EPSG","code":8807}}]},"coordinate_system":{"subtype":"Cartesian","axis":[{"name":"Easting","abbreviation":"X","direction":"east","unit":"metre"},{"name":"Northing","abbreviation":"Y","direction":"north","unit":"metre"}]},"scope":"Web mapping and visualisation.","area":"World between 85.06°S and 85.06°N.","bbox":{"south_latitude":-85.06,"west_longitude":-180,"north_latitude":85.06,"east_longitude":180},"id":{"authority":"EPSG","code":3857}}`
