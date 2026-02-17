// ── Time Axis Label Generator ───────────────────────────────────────────────
// Self-contained hierarchical tick generator for the SDT trace visualization.
//
// Modified to support multi-line labels (date on line 1, time on line 2)
// ────────────────────────────────────────────────────────────────────────────

var SdtTimeAxis = (function () {
  'use strict';

  var MIN_PX = 70; // minimum pixel distance between adjacent labels

  var USEC_SEC  = 1000000;
  var USEC_MIN  = 60 * USEC_SEC;
  var USEC_HOUR = 60 * USEC_MIN;
  var USEC_DAY  = 24 * USEC_HOUR;

  function usToDate(us) {
    return new Date(us / 1000);
  }

  function dateToUs(d) {
    return d.getTime() * 1000;
  }

  function floorDay(d) {
    return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  }

  function floorMonth(d) {
    return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1));
  }

  function floorYear(d) {
    return new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  }

  function enumerateIntraDay(tMinUs, tMaxUs, offsets) {
    var results = [];
    var startDay = floorDay(usToDate(tMinUs));
    var endDay   = floorDay(usToDate(tMaxUs));
    startDay.setUTCDate(startDay.getUTCDate() - 1);
    endDay.setUTCDate(endDay.getUTCDate() + 1);

    for (var d = new Date(startDay); d <= endDay; d.setUTCDate(d.getUTCDate() + 1)) {
      var dayUs = dateToUs(d);
      for (var i = 0; i < offsets.length; i++) {
        var t = dayUs + offsets[i];
        if (t >= tMinUs && t <= tMaxUs) {
          results.push(t);
        }
      }
    }
    return results;
  }

  function enumerateIntraHour(tMinUs, tMaxUs, minuteOffsets) {
    var results = [];
    var startHour = new Date(usToDate(tMinUs));
    startHour.setUTCMinutes(0, 0, 0);
    startHour.setUTCHours(startHour.getUTCHours() - 1);
    var endHour = new Date(usToDate(tMaxUs));
    endHour.setUTCMinutes(0, 0, 0);
    endHour.setUTCHours(endHour.getUTCHours() + 1);

    for (var h = new Date(startHour); h <= endHour; h.setUTCHours(h.getUTCHours() + 1)) {
      for (var i = 0; i < minuteOffsets.length; i++) {
        var t = dateToUs(h) + minuteOffsets[i] * USEC_MIN;
        if (t >= tMinUs && t <= tMaxUs) {
          results.push(t);
        }
      }
    }
    return results;
  }

  var TIERS = [
    {
      name: 'year',
      enumerate: function (tMinUs, tMaxUs) {
        var results = [];
        var startYear = floorYear(usToDate(tMinUs));
        startYear.setUTCFullYear(startYear.getUTCFullYear() - 1);
        var endYear = floorYear(usToDate(tMaxUs));
        endYear.setUTCFullYear(endYear.getUTCFullYear() + 1);
        for (var d = new Date(startYear); d <= endYear; d.setUTCFullYear(d.getUTCFullYear() + 1)) {
          var t = dateToUs(d);
          if (t >= tMinUs && t <= tMaxUs) results.push(t);
        }
        return results;
      },
      format: 'year'
    },
    {
      name: 'quarter',
      enumerate: function (tMinUs, tMaxUs) {
        var results = [];
        var startDate = floorMonth(usToDate(tMinUs));
        startDate.setUTCMonth(startDate.getUTCMonth() - 3);
        var endDate = floorMonth(usToDate(tMaxUs));
        endDate.setUTCMonth(endDate.getUTCMonth() + 3);
        for (var d = new Date(startDate); d <= endDate; d.setUTCMonth(d.getUTCMonth() + 3)) {
          var t = dateToUs(d);
          if (t >= tMinUs && t <= tMaxUs) results.push(t);
        }
        return results;
      },
      format: 'date'
    },
    {
      name: 'month',
      enumerate: function (tMinUs, tMaxUs) {
        var results = [];
        var startDate = floorMonth(usToDate(tMinUs));
        startDate.setUTCMonth(startDate.getUTCMonth() - 1);
        var endDate = floorMonth(usToDate(tMaxUs));
        endDate.setUTCMonth(endDate.getUTCMonth() + 1);
        for (var d = new Date(startDate); d <= endDate; d.setUTCMonth(d.getUTCMonth() + 1)) {
          var t = dateToUs(d);
          if (t >= tMinUs && t <= tMaxUs) results.push(t);
        }
        return results;
      },
      format: 'date'
    },
    {
      name: 'other-months',
      enumerate: function (tMinUs, tMaxUs) {
        var results = [];
        var startDate = floorMonth(usToDate(tMinUs));
        startDate.setUTCMonth(startDate.getUTCMonth() - 1);
        var endDate = floorMonth(usToDate(tMaxUs));
        endDate.setUTCMonth(endDate.getUTCMonth() + 1);
        var covered = {0:1, 3:1, 6:1, 9:1};
        for (var d = new Date(startDate); d <= endDate; d.setUTCMonth(d.getUTCMonth() + 1)) {
          if (covered[d.getUTCMonth()]) continue;
          var t = dateToUs(new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1)));
          if (t >= tMinUs && t <= tMaxUs) results.push(t);
        }
        return results;
      },
      format: 'date'
    },
    {
      name: 'half-month',
      enumerate: function (tMinUs, tMaxUs) {
        var results = [];
        var startDate = floorMonth(usToDate(tMinUs));
        startDate.setUTCMonth(startDate.getUTCMonth() - 1);
        var endDate = floorMonth(usToDate(tMaxUs));
        endDate.setUTCMonth(endDate.getUTCMonth() + 1);
        for (var d = new Date(startDate); d <= endDate; d.setUTCMonth(d.getUTCMonth() + 1)) {
          var t = dateToUs(new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 15)));
          if (t >= tMinUs && t <= tMaxUs) results.push(t);
        }
        return results;
      },
      format: 'day'
    },
    {
      name: 'quarter-month',
      enumerate: function (tMinUs, tMaxUs) {
        var results = [];
        var startDate = floorMonth(usToDate(tMinUs));
        startDate.setUTCMonth(startDate.getUTCMonth() - 1);
        var endDate = floorMonth(usToDate(tMaxUs));
        endDate.setUTCMonth(endDate.getUTCMonth() + 1);
        for (var d = new Date(startDate); d <= endDate; d.setUTCMonth(d.getUTCMonth() + 1)) {
          var days = [8, 22];
          for (var i = 0; i < days.length; i++) {
            var t = dateToUs(new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), days[i])));
            if (t >= tMinUs && t <= tMaxUs) results.push(t);
          }
        }
        return results;
      },
      format: 'day'
    },
    {
      name: 'day',
      enumerate: function (tMinUs, tMaxUs) {
        return enumerateIntraDay(tMinUs, tMaxUs, [0]);
      },
      format: 'day'
    },
    {
      name: 'half-day',
      enumerate: function (tMinUs, tMaxUs) {
        return enumerateIntraDay(tMinUs, tMaxUs, [12 * USEC_HOUR]);
      },
      format: 'day-time'
    },
    {
      name: 'quarter-day',
      enumerate: function (tMinUs, tMaxUs) {
        return enumerateIntraDay(tMinUs, tMaxUs, [6 * USEC_HOUR, 18 * USEC_HOUR]);
      },
      format: 'day-time'
    },
    {
      name: '4-hour',
      enumerate: function (tMinUs, tMaxUs) {
        return enumerateIntraDay(tMinUs, tMaxUs, [4, 8, 16, 20].map(function(h) { return h * USEC_HOUR; }));
      },
      format: 'day-time'
    },
    {
      name: 'hour',
      enumerate: function (tMinUs, tMaxUs) {
        var offsets = [];
        for (var h = 1; h < 24; h++) offsets.push(h * USEC_HOUR);
        return enumerateIntraDay(tMinUs, tMaxUs, offsets);
      },
      format: 'day-time'
    },
    {
      name: '30-minute',
      enumerate: function (tMinUs, tMaxUs) {
        return enumerateIntraHour(tMinUs, tMaxUs, [30]);
      },
      format: 'day-time'
    },
    {
      name: '15-minute',
      enumerate: function (tMinUs, tMaxUs) {
        return enumerateIntraHour(tMinUs, tMaxUs, [15, 45]);
      },
      format: 'day-time'
    },
    {
      name: '10-minute',
      enumerate: function (tMinUs, tMaxUs) {
        return enumerateIntraHour(tMinUs, tMaxUs, [10, 20, 40, 50]);
      },
      format: 'day-time'
    },
    {
      name: '5-minute',
      enumerate: function (tMinUs, tMaxUs) {
        return enumerateIntraHour(tMinUs, tMaxUs, [5, 25, 35, 55]);
      },
      format: 'day-time'
    },
    {
      name: 'minute',
      enumerate: function (tMinUs, tMaxUs) {
        var minutes = [];
        for (var m = 1; m < 60; m++) {
          if (m % 5 !== 0) minutes.push(m);
        }
        return enumerateIntraHour(tMinUs, tMaxUs, minutes);
      },
      format: 'time-sec'
    }
  ];

  function pad2(n) { return n < 10 ? '0' + n : '' + n; }

  var MONTHS = ['Jan','Feb','Mar','Apr','May','Jun',
                'Jul','Aug','Sep','Oct','Nov','Dec'];

  // Modified formatters to return { line1, line2 } for multi-line labels
  var FORMATTERS = {
    'year': function (us) {
      return {
        line1: '' + usToDate(us).getUTCFullYear(),
        line2: ''
      };
    },
    'date': function (us) {
      var d = usToDate(us);
      return {
        line1: d.getUTCFullYear() + '-' + pad2(d.getUTCMonth() + 1) + '-' + pad2(d.getUTCDate()),
        line2: ''
      };
    },
    'day': function (us) {
      var d = usToDate(us);
      return {
        line1: MONTHS[d.getUTCMonth()] + ' ' + d.getUTCDate(),
        line2: ''
      };
    },
    'day-time': function (us) {
      var d = usToDate(us);
      return {
        line1: MONTHS[d.getUTCMonth()] + ' ' + d.getUTCDate(),
        line2: pad2(d.getUTCHours()) + ':' + pad2(d.getUTCMinutes())
      };
    },
    'time': function (us) {
      var d = usToDate(us);
      return {
        line1: '',
        line2: pad2(d.getUTCHours()) + ':' + pad2(d.getUTCMinutes())
      };
    },
    'time-sec': function (us) {
      var d = usToDate(us);
      return {
        line1: '',
        line2: pad2(d.getUTCHours()) + ':' + pad2(d.getUTCMinutes()) + ':' + pad2(d.getUTCSeconds())
      };
    }
  };

  function chooseFormatter(ticks, finestFormat) {
    // For day-time format, check if all ticks share the same date
    if (finestFormat === 'day-time' || finestFormat === 'day') {
      var allSameDate = true;
      if (ticks.length > 1) {
        var firstDate = usToDate(ticks[0]);
        var firstDay = firstDate.getUTCFullYear() * 10000 +
                       firstDate.getUTCMonth() * 100 +
                       firstDate.getUTCDate();
        for (var i = 1; i < ticks.length; i++) {
          var d = usToDate(ticks[i]);
          var day = d.getUTCFullYear() * 10000 + d.getUTCMonth() * 100 + d.getUTCDate();
          if (day !== firstDay) { allSameDate = false; break; }
        }
      }
      if (allSameDate && finestFormat === 'day-time') return FORMATTERS['time'];
    }
    return FORMATTERS[finestFormat] || FORMATTERS['date'];
  }

  function continuousPx(tUs, tMin, zoom, timeScale) {
    return zoom * (tUs - tMin) / timeScale;
  }

  function checkSpacing(sorted, tMin, zoom, timeScale) {
    for (var i = 1; i < sorted.length; i++) {
      var px1 = continuousPx(sorted[i - 1], tMin, zoom, timeScale);
      var px2 = continuousPx(sorted[i], tMin, zoom, timeScale);
      if (px2 - px1 < MIN_PX) return false;
    }
    return true;
  }

  function dedupeSort(arr) {
    if (arr.length === 0) return arr;
    arr.sort(function (a, b) { return a - b; });
    var out = [arr[0]];
    for (var i = 1; i < arr.length; i++) {
      if (arr[i] !== arr[i - 1]) out.push(arr[i]);
    }
    return out;
  }

  function generateTicks(tMin, tMax, zoom, timeScale) {
    var accepted = [];
    var finestFormat = 'date';

    for (var i = 0; i < TIERS.length; i++) {
      var newPositions = TIERS[i].enumerate(tMin, tMax);
      if (newPositions.length === 0) continue;

      var merged = dedupeSort(accepted.concat(newPositions));

      if (merged.length < 2) {
        accepted = merged;
        finestFormat = TIERS[i].format;
        continue;
      }

      if (checkSpacing(merged, tMin, zoom, timeScale)) {
        accepted = merged;
        finestFormat = TIERS[i].format;
      } else {
        break;
      }
    }

    if (accepted.length < 2) {
      return {
        ticks: [tMin, tMax],
        format: FORMATTERS['date']
      };
    }

    var formatter = chooseFormatter(accepted, finestFormat);
    return {
      ticks: accepted,
      format: formatter
    };
  }

  return {
    generateTicks: generateTicks
  };

})();