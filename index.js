var fs = require('fs');
var csv = require('csv-parse');
var parse = require('csv-parse/lib/sync');
var moment = require('moment');
var gju = require('geojson-utils');

// Import GIS data.
require.extensions[ '.geojson' ] = require.extensions[ '.json' ];
function requireJSON(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
};

var geo_neighborhoods = requireJSON('data_sources/cambridgegis/Boundary/CDD_Neighborhoods/BOUNDARY_CDDNeighborhoods.geojson');
// Import raw data.
var data_accidents_2010_2013 = parse(fs.readFileSync('data_sources/ACCIDENT-2010-2013.csv', 'utf8'), { columns: true });
var data_accidents_2014 = parse(fs.readFileSync('data_sources/ACCIDENT-2014.csv', 'utf8'), { columns: true });
var data_weather = parse(fs.readFileSync('data_sources/weather-2010-2014.csv', 'utf8'), { columns: true });

/*
 * Combine accidents data and transform.
 */
function transformAccidentData(d) {
    return {
        coordinates: [ parseFloat(d[ 'Longitude' ]), parseFloat(d[ 'Latitude' ]) ],
        location: d[ 'Location' ] || d[ 'LOCATION' ],
        date: moment(d[ 'Date Time' ], 'MM/DD/YYYY HH:mm:ss A').toDate(),
        day: d[ 'Day Of Week' ] || d[ 'Day of Week' ],
        object1: d[ 'Object 1' ],
        object2: d[ 'Object 2' ]
    };
}
var data_accidents = data_accidents_2010_2013.map(transformAccidentData)
    .concat(data_accidents_2014.map(transformAccidentData));

// Precalculate choropleth values for neighborhoods and embed in feature properties.
var _numAccidents = {};
var _maxAccidents = 0;
geo_neighborhoods.features.forEach(function(d) {
    var id = d.properties[ 'N_HOOD' ];
    _numAccidents[ id ] = 0;
});
data_accidents.forEach(function(d) {
    var point = { type: 'Point', coordinates: d.coordinates };
    for (var i = 0; i < geo_neighborhoods.features.length; i++) {
        var feature = geo_neighborhoods.features[ i ];
        var polygon = feature.geometry;
        if (gju.pointInPolygon(point, polygon)) {
            var id = feature.properties[ 'N_HOOD' ];
            if (++_numAccidents[ id ] > _maxAccidents) {
                _maxAccidents = _numAccidents[ id ];
            }
            return;
        }
    }
    //console.log('accident not in neighborhood');
});
this.accidentLevels = {};
geo_neighborhoods.features.forEach(function(d) {
    var id = d.properties[ 'N_HOOD' ];
    d.properties.accidentRating = (_numAccidents[ id ] / _maxAccidents) || 0;
});
console.log(geo_neighborhoods.features[ 0 ].properties);
////////TODO:write to geojson

// Compile accidents data.
// - Relate weather data.

data_weather = data_weather.map(function(d) {
    console.log(d);
    var events = d[ ' Events' ].split('-');
    return {
        date: moment(d[ 'EST' ], 'YYYY-M-D').toDate(),
        // Max TemperatureF,Mean TemperatureF,Min TemperatureF
        // Max VisibilityMiles, Mean VisibilityMiles, Min VisibilityMiles
        precipInches: d[ 'PrecipitationIn' ],//this is sometimes 'T', what does this mean?
        events: {
            fog: events.indexOf('Fog') !== -1,
            rain: events.indexOf('Rain') !== -1,
            thunderstorm: events.indexOf('Thunderstorm') !== -1,
            snow: events.indexOf('Snow') !== -1,
            hail: events.indexOf('Hail') !== -1
        }
    };
});
console.log(data_weather[ 0 ]);
