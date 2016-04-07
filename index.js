var path = require('path');
var fs = require('fs');
var csv = require('csv-parse');
var parse = require('csv-parse/lib/sync');
var moment = require('moment');
var gju = require('geojson-utils');

process.stdout.write('[STARTING]\n');


//--------------------------------------------------------------------------------------------------
// Load Raw Data
//--------------------------------------------------------------------------------------------------

process.stdout.write('    Loading raw data... ');

// Import GIS data.
var geo_neighborhoods = JSON.parse(fs.readFileSync('data_sources/BOUNDARY_Neighborhoods.geojson', 'utf8'));

// Import CSV data.
var data_accidents_2010_2013 = parse(fs.readFileSync('data_sources/ACCIDENT_2010-2013.csv', 'utf8'), { columns: true });
var data_accidents_2014 = parse(fs.readFileSync('data_sources/ACCIDENT_2014.csv', 'utf8'), { columns: true });
var data_weather_2010_2014 = parse(fs.readFileSync('data_sources/WEATHER_2010-2014.csv', 'utf8'), { columns: true });
var data_citations_2010_2014 = parse(fs.readFileSync('data_sources/CITATION_2010-2014.csv', 'utf8'), { columns: true });

// Import text data.
function parseSunset(year, text) {
    var data = [];
    text.split('\n').slice(9, 40)
        .forEach(function(r, day_index) {
            r.match(/[\ ]{2}[0-9 ]{4}\ [0-9 ]{4}/g)
                .forEach(function(c, month_index) {
                    c = c.trim();
                    if (!c) { return; }
                    var values = c.split(' ');
                    var date = new Date(year, month_index, day_index + 1);
                    var sunriseTime = moment(values[ 0 ], 'HHmm').toDate();
                    var sunriseDate = new Date(date.getTime());
                    sunriseDate.setHours(sunriseTime.getHours(), sunriseTime.getMinutes());
                    var sunsetTime = moment(values[ 1 ], 'HHmm').toDate();
                    var sunsetDate = new Date(date.getTime());
                    sunsetDate.setHours(sunsetTime.getHours(), sunsetTime.getMinutes());
                    data.push({
                        date: date,
                        sunrise: sunriseDate,
                        sunset: sunsetDate
                    });
                });
        });
    return data;
}
var data_sunset_2010 = parseSunset(2010, fs.readFileSync('data_sources/SUNSET_2010.txt', 'utf8'));
var data_sunset_2011 = parseSunset(2011, fs.readFileSync('data_sources/SUNSET_2011.txt', 'utf8'));
var data_sunset_2012 = parseSunset(2012, fs.readFileSync('data_sources/SUNSET_2012.txt', 'utf8'));
var data_sunset_2013 = parseSunset(2013, fs.readFileSync('data_sources/SUNSET_2013.txt', 'utf8'));
var data_sunset_2014 = parseSunset(2014, fs.readFileSync('data_sources/SUNSET_2014.txt', 'utf8'));

process.stdout.write('done\n');


//--------------------------------------------------------------------------------------------------
// Accidents
//--------------------------------------------------------------------------------------------------

process.stdout.write('    Processing accident data... ');

var data_accidents = data_accidents_2010_2013.concat(data_accidents_2014).map(function(d) {
    // Build the accident data object.
    var obj = {
        coordinates: [ parseFloat(d[ 'Longitude' ]), parseFloat(d[ 'Latitude' ]) ],
        location: (d[ 'Location' ] || d[ 'LOCATION' ]).trim(),
        date: moment(d[ 'Date Time' ], 'MM/DD/YYYY HH:mm:ss A').toDate(),
        day: d[ 'Day Of Week' ] || d[ 'Day of Week' ],
        object1: d[ 'Object 1' ],
        object2: d[ 'Object 2' ],
        neighborhood: null
    };
    // Determine in which neighborhood this accident occurred.
    var point = { type: 'Point', coordinates: obj.coordinates };
    for (var i = 0; i < geo_neighborhoods.features.length; i++) {
        var feature = geo_neighborhoods.features[ i ];
        var polygon = feature.geometry;
        if (gju.pointInPolygon(point, polygon)) {
            obj.neighborhood = feature.properties[ 'N_HOOD' ];
            break;
        }
    }
    // Return the accident data object.
    return obj;
});
data_accidents.sort(function(a, b) { return a.date.getTime() - b.date.getTime(); });

var data_accidents_output_path = path.resolve(__dirname, 'data_output/cambridge_accidents_2010-2014.json');
fs.writeFileSync(data_accidents_output_path, JSON.stringify(data_accidents));

process.stdout.write('done\n');
process.stdout.write('        > ' + data_accidents_output_path + '\n');


//--------------------------------------------------------------------------------------------------
// Citations
//--------------------------------------------------------------------------------------------------

var CITATION_LABELS = {
    SPEEDING: 'Speeding',
    YIELD: 'Failure to Yield'
};
var CITATION_MAPPING = {
    'SPEEDING * C90 S17': CITATION_LABELS.SPEEDING,
    'SPEEDING IN VIOL SPECIAL REGULATION * C90 S18': CITATION_LABELS.SPEEDING,
    'STOP/YIELD, FAIL TO * C89 S9': CITATION_LABELS.YIELD,
    'YIELD AT INTERSECTION, FAIL * C89 S8': CITATION_LABELS.YIELD/*,
    'NEGLIGENT OPERATION OF MOTOR VEHICLE c90 S24': ,
    'TURN, IMPROPER * C90 S14': '',
    'VIOLATION OF POSTED SIGN': ''*/
};

var data_citations = data_citations_2010_2014.map(function(d) {
/*
'Citation Number': 'M7461813       ',
'Date Time Issued': '01/01/2010 01:51:00 AM',
'Street Number': '',
'Street Name': 'MASSACHUSETTS AVE             ',
'Cross Street': 'RINDGE AVE                    ',
'Charge Code': '90/24/J        ',
'Charge Description': 'OUI-LIQUOR C90 S24                                '
*/
    return {
        date: moment(d[ 'Date Time Issued' ], 'MM/DD/YYYY HH:mm:ss A').toDate(),
        label: CITATION_LABELS[ d[ 'Charge Description' ] ]
    };
    //TODO: get lat long with cross street?
})/*.filter(function(d) {
    return d.label !== undefined;
})*/;
//TODO: sort


//--------------------------------------------------------------------------------------------------
// Weather
//--------------------------------------------------------------------------------------------------

process.stdout.write('    Processing weather data... ');

// Create a lookup for sunrise / sunset times.
var data_sunset = [
    data_sunset_2010,
    data_sunset_2011,
    data_sunset_2012,
    data_sunset_2013,
    data_sunset_2014
].reduce(function(a, b) { return a.concat(b); }, []);
var data_sunset_lookup = {};
data_sunset.forEach(function(d) {
    data_sunset_lookup[ moment(d.date).format('YYYY-MM-DD') ] = d;
});

// Process the weather data.
var data_weather = data_weather_2010_2014.map(function(d) {
    var date = moment(d[ 'EST' ], 'YYYY-M-D').toDate();
    var d_sunset = data_sunset_lookup[ moment(date).format('YYYY-MM-DD') ];
    var events = d[ ' Events' ].split('-');
    return {
        date: date,
        sunrise: d_sunset.sunrise,
        sunset: d_sunset.sunset,
        temperature: {
            min: parseInt(d[ 'Min TemperatureF' ], 10),
            max: parseInt(d[ 'Max TemperatureF' ], 10),
            mean: parseInt(d[ 'Mean TemperatureF' ], 10)
        },
        visibility_Miles: {
            min: parseInt(d[ ' Min VisibilityMiles' ], 10),
            max: parseInt(d[ ' Max VisibilityMiles' ], 10),
            mean: parseInt(d[ ' Mean VisibilityMiles' ], 10)
        },
        precipitation_Inches: parseInt(d[ 'PrecipitationIn' ], 10) || 0,
        events: {
            fog: events.indexOf('Fog') !== -1,
            rain: events.indexOf('Rain') !== -1,
            thunderstorm: events.indexOf('Thunderstorm') !== -1,
            snow: events.indexOf('Snow') !== -1,
            hail: events.indexOf('Hail') !== -1
        }
    };
});
data_weather.sort(function(a, b) { return a.date.getTime() - b.date.getTime(); });

var data_weather_output_path = path.resolve(__dirname, 'data_output/cambridge_weather_2010-2014.json');
fs.writeFileSync(data_weather_output_path, JSON.stringify(data_weather));

process.stdout.write('done\n');
process.stdout.write('        > ' + data_weather_output_path + '\n');


process.stdout.write('[COMPLETE]\n');
