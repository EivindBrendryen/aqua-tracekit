// ── SDT Chart Renderer ──────────────────────────────────────────────────────
// Self-contained chart module for the SDT trace visualization.
// Handles: coordinate mapping, SVG rebuild, zoom controls, interactive tracing.
//
// Modified to support:
// - Multi-line x-axis labels (date on line 1, time on line 2)
// - Interactive population selection with backward/forward tracing
//
// Usage (from Rust-injected inline script):
//   SdtChart.create({ zoom: 1.0, tMin: ..., tMax: ..., ... });
//
// Depends on: SdtTimeAxis (must be loaded first)
// ────────────────────────────────────────────────────────────────────────────

var SdtChart = (function () {
  'use strict';

  function create(cfg) {
    var currentZoom = cfg.zoom;
    var baseZoom = cfg.zoom;
    var tMin = cfg.tMin;
    var tMax = cfg.tMax;
    var timeScale = cfg.timeScale;
    var gapPx = cfg.gapPx;
    var transferTimes = cfg.transferTimes;
    var marginLeft = cfg.marginLeft;
    var marginTop = cfg.marginTop;
    var marginRight = cfg.marginRight;
    var marginBottom = cfg.marginBottom;
    var laneHeight = cfg.laneHeight;
    var numLanes = cfg.numLanes;
    var rectPadding = cfg.rectPadding;
    var populations = cfg.populations;
    var transfers_data = cfg.transfers;
    var lanes_data = cfg.lanes;

    // ── Selection state ───────────────────────────────────────────────
    var selectedPopId = null;
    var tracedPopIds = new Set();

    // ── Color configuration ───────────────────────────────────────────
    var COLORS = {
      default: { fill: '#4dabf7', stroke: '#339af0' },
      selected: { fill: '#f59f00', stroke: '#e67700' },
      traced: { fill: '#ffb366', stroke: '#f59f00' }
    };

    // ── Coordinate mapping ────────────────────────────────────────────

    // gapMode: true/'after' (rect starts), false/'before' (rect ends), 'middle' (labels)
    function timeToX(tUs, gapMode) {
      var continuous = currentZoom * (tUs - tMin) / timeScale;
      if (gapMode === 'middle') {
        var lt = 0;
        var onGap = false;
        for (var i = 0; i < transferTimes.length; i++) {
          if (transferTimes[i] < tUs) lt++;
          else if (transferTimes[i] === tUs) { onGap = true; break; }
          else break;
        }
        if (onGap) return continuous + lt * gapPx + gapPx / 2;
        return continuous + lt * gapPx;
      }
      var afterGap = (gapMode === 'after') || (gapMode === true);
      var gapCount = 0;
      for (var i = 0; i < transferTimes.length; i++) {
        if (afterGap ? transferTimes[i] <= tUs : transferTimes[i] < tUs) gapCount++;
        else break;
      }
      return continuous + gapCount * gapPx;
    }

    // ── Timestamp formatting (for tooltips) ───────────────────────────

    function pad2(n) { return n < 10 ? '0' + n : '' + n; }

    function formatTimestamp(us) {
      var d = new Date(us / 1000);
      return d.getUTCFullYear() + '-' +
        pad2(d.getUTCMonth() + 1) + '-' +
        pad2(d.getUTCDate()) + ' ' +
        pad2(d.getUTCHours()) + ':' +
        pad2(d.getUTCMinutes());
    }

    // ── SVG helpers ───────────────────────────────────────────────────

    var SVG_NS = 'http://www.w3.org/2000/svg';

    function svgEl(tag, attrs) {
      var el = document.createElementNS(SVG_NS, tag);
      for (var k in attrs) {
        if (attrs.hasOwnProperty(k)) el.setAttribute(k, attrs[k]);
      }
      return el;
    }

    // ── Tracing functions ─────────────────────────────────────────────

    function traceBackward(popId, visited) {
      visited = visited || new Set();
      if (visited.has(popId)) return visited;
      visited.add(popId);

      for (var i = 0; i < transfers_data.length; i++) {
        if (transfers_data[i].dest_pop_id === popId) {
          traceBackward(transfers_data[i].source_pop_id, visited);
        }
      }
      return visited;
    }

    function traceForward(popId, visited) {
      visited = visited || new Set();
      if (visited.has(popId)) return visited;
      visited.add(popId);

      for (var i = 0; i < transfers_data.length; i++) {
        if (transfers_data[i].source_pop_id === popId) {
          traceForward(transfers_data[i].dest_pop_id, visited);
        }
      }
      return visited;
    }

    function buildTraceSet(popId) {
      var backward = traceBackward(popId);
      var forward = traceForward(popId);

      // Combine both sets, excluding the selected population itself
      var combined = new Set();
      backward.forEach(function (id) { if (id !== popId) combined.add(id); });
      forward.forEach(function (id) { if (id !== popId) combined.add(id); });

      return combined;
    }

    // ── Color selection helper ────────────────────────────────────────

    function getPopulationColors(popId) {
      if (popId === selectedPopId) {
        return COLORS.selected;
      } else if (tracedPopIds.has(popId)) {
        return COLORS.traced;
      } else {
        return COLORS.default;
      }
    }

    // ── Selection handlers ────────────────────────────────────────────

    function selectPopulation(popId) {
      selectedPopId = popId;
      tracedPopIds = buildTraceSet(popId);
      rebuild();
    }

    function clearSelection() {
      selectedPopId = null;
      tracedPopIds.clear();
      rebuild();
    }

    // ── Rebuild ───────────────────────────────────────────────────────

    function rebuild() {
      var contentWidth = timeToX(tMax, true) + marginRight;
      var totalWidth = marginLeft + contentWidth;
      var totalHeight = marginTop + numLanes * laneHeight + marginBottom;

      var svg = document.getElementById('sdt-svg');
      svg.setAttribute('width', totalWidth);
      svg.setAttribute('height', totalHeight);

      // Keep <style> and <defs>, remove everything else
      while (svg.children.length > 2) svg.removeChild(svg.lastChild);

      // Add click handler to SVG background for deselection
      var background = svgEl('rect', {
        x: 0, y: 0, width: totalWidth, height: totalHeight,
        fill: 'transparent', cursor: 'default'
      });
      background.addEventListener('click', clearSelection);
      svg.appendChild(background);

      // ── Lane backgrounds + labels ──
      for (var i = 0; i < numLanes; i++) {
        var y = marginTop + i * laneHeight;
        svg.appendChild(svgEl('rect', {
          x: 0, y: y, width: totalWidth, height: laneHeight,
          fill: i % 2 === 0 ? '#f8f9fa' : '#ffffff',
          'pointer-events': 'none'
        }));
        var txt = svgEl('text', {
          x: marginLeft - 8, y: y + laneHeight / 2 + 4,
          'class': 'lane-label'
        });
        txt.textContent = lanes_data[i].label;
        svg.appendChild(txt);
      }

      // ── Lane separators ──
      for (var i = 0; i <= numLanes; i++) {
        var y = marginTop + i * laneHeight;
        svg.appendChild(svgEl('line', {
          x1: 0, y1: y, x2: totalWidth, y2: y,
          stroke: '#dee2e6', 'stroke-width': 1,
          'pointer-events': 'none'
        }));
      }

      // ── Transfer time gap indicators ──
      for (var i = 0; i < transferTimes.length; i++) {
        var x = marginLeft + timeToX(transferTimes[i], false) + gapPx / 2;
        svg.appendChild(svgEl('line', {
          x1: x, y1: marginTop, x2: x, y2: totalHeight - marginBottom,
          stroke: '#e0e0e0', 'stroke-width': 1, 'stroke-dasharray': '4,4',
          'pointer-events': 'none'
        }));
      }

      // ── Time axis labels (hierarchical, zoom-adaptive, multi-line) ──
      var axisResult = SdtTimeAxis.generateTicks(tMin, tMax, currentZoom, timeScale);
      for (var i = 0; i < axisResult.ticks.length; i++) {
        var t = axisResult.ticks[i];
        var x = marginLeft + timeToX(t, 'middle');

        // Create text element with multi-line support
        var txt = svgEl('text', {
          x: x, y: marginTop - 18, 'class': 'time-label'
        });

        var formatted = axisResult.format(t);

        // Add line 1 (date) if present
        if (formatted.line1) {
          var tspan1 = svgEl('tspan', { x: x, dy: 0 });
          tspan1.textContent = formatted.line1;
          txt.appendChild(tspan1);
        }

        // Add line 2 (time) if present
        if (formatted.line2) {
          var tspan2 = svgEl('tspan', { x: x, dy: '1.2em' });
          tspan2.textContent = formatted.line2;
          txt.appendChild(tspan2);
        }

        svg.appendChild(txt);

        svg.appendChild(svgEl('line', {
          x1: x, y1: marginTop - 4, x2: x, y2: marginTop,
          stroke: '#adb5bd', 'stroke-width': 1,
          'pointer-events': 'none'
        }));
      }

      // ── Population rectangles ──
      var laneMap = {};
      for (var i = 0; i < lanes_data.length; i++) laneMap[lanes_data[i].container_id] = i;

      var popPositions = {};
      for (var i = 0; i < populations.length; i++) {
        var p = populations[i];
        var li = laneMap[p.container_id];
        if (li === undefined) continue;
        var x = marginLeft + timeToX(p.start_us, true);
        var x2 = marginLeft + timeToX(p.end_us, false);
        var w = Math.max(x2 - x, 2);
        var ry = marginTop + li * laneHeight + rectPadding;
        var h = laneHeight - 2 * rectPadding;

        popPositions[p.pop_id] = { x: x, x2: x2, y: ry, h: h, lane: li };

        // Get colors based on selection state
        var colors = getPopulationColors(p.pop_id);

        var rect = svgEl('rect', {
          x: x, y: ry, width: w, height: h, rx: 3,
          fill: colors.fill,
          stroke: colors.stroke,
          'stroke-width': p.pop_id === selectedPopId ? 2 : 1,
          cursor: 'pointer'
        });

        var tip = p.pop_id + '\n' + formatTimestamp(p.start_us) + ' → ' + formatTimestamp(p.end_us);
        if (p.tooltip) tip += '\n' + p.tooltip;
        var title = svgEl('title', {});
        title.textContent = tip;
        rect.appendChild(title);

        // Add click handler for selection
        (function (popId) {
          rect.addEventListener('click', function (e) {
            e.stopPropagation();
            selectPopulation(popId);
          });
        })(p.pop_id);

        // Add hover effect
        rect.addEventListener('mouseenter', function () {
          if (this.getAttribute('fill') === COLORS.default.fill) {
            this.setAttribute('fill', '#339af0');
          }
        });
        rect.addEventListener('mouseleave', function () {
          rebuild(); // Reapply proper colors
        });

        svg.appendChild(rect);

        if (p.label && w > 30) {
          var lbl = svgEl('text', {
            x: x + 4, y: ry + h / 2 + 4, 'class': 'pop-label'
          });
          lbl.textContent = p.label;
          svg.appendChild(lbl);
        }
      }

      // ── Transfer arrows ──
      for (var i = 0; i < transfers_data.length; i++) {
        var tr = transfers_data[i];
        var src = popPositions[tr.source_pop_id];
        var dst = popPositions[tr.dest_pop_id];
        if (!src || !dst) continue;

        var tx1 = marginLeft + timeToX(tr.transfer_time_us, false);
        var tx2 = marginLeft + timeToX(tr.transfer_time_us, true);
        var srcY = marginTop + src.lane * laneHeight + laneHeight / 2;
        var dstY = marginTop + dst.lane * laneHeight + laneHeight / 2;

        var el = svgEl('line', {
          x1: tx1, y1: srcY, x2: tx2, y2: dstY,
          stroke: '#e74c3c', 'stroke-width': 1.5,
          'marker-end': 'url(#arrowhead)', 'class': 'transfer-arrow'
        });
        var tip = tr.source_pop_id + ' → ' + tr.dest_pop_id + '\n' + formatTimestamp(tr.transfer_time_us);
        if (tr.tooltip) tip += '\n' + tr.tooltip;
        var title = svgEl('title', {});
        title.textContent = tip;
        el.appendChild(title);
        svg.appendChild(el);
      }

      document.getElementById('sdt-zoom-label').textContent = currentZoom.toFixed(1) + 'x';
    }

    // ── Zoom controls (exposed globally for onclick handlers) ─────────

    window.sdtZoom = function (factor) {
      currentZoom *= factor;
      rebuild();
    };

    window.sdtResetZoom = function () {
      currentZoom = baseZoom;
      rebuild();
    };

    // ── Selection controls (exposed globally for notebook calls) ──────

    window.sdtSelectPopulation = function (popId) {
      selectPopulation(popId);
    };

    window.sdtClearSelection = function () {
      clearSelection();
    };

    // Initial render
    rebuild();
  }

  // ── Public API ──────────────────────────────────────────────────────
  return { create: create };

})();