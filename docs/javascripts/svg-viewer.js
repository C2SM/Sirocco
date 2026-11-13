(function () {
  const STEP = 1.25, MIN = 0.25, MAX = 8;
  const state = new WeakMap(); // viewer -> {zoom, initial, ratio}

  function apply(viewer) {
    const s = state.get(viewer);
    const viewport = viewer.querySelector(".svg-viewer__viewport");
    const obj = viewport.querySelector("object");
    if (!s || !obj) return;
    const width = viewport.clientWidth * s.zoom;
    obj.style.width = width + "px";
    obj.style.height = width * s.ratio + "px";
  }

  function init(obj) {
    const viewer = obj.closest(".svg-viewer");
    if (!viewer) return;
    const svg = obj.contentDocument && obj.contentDocument.documentElement;
    let ratio = 0.6;
    if (svg) {
      const vb = svg.viewBox && svg.viewBox.baseVal;
      if (vb && vb.width) ratio = vb.height / vb.width;
      else if (svg.width && svg.height)
        ratio = svg.height.baseVal.value / svg.width.baseVal.value;
    }
    // Initial zoom from data-zoom, clamped to [MIN, MAX], default 1
    const requested = parseFloat(viewer.dataset.zoom);
    const initial = Number.isFinite(requested)
      ? Math.min(MAX, Math.max(MIN, requested))
      : 1;
    state.set(viewer, { zoom: initial, initial, ratio });
    apply(viewer);
  }

  document.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-svg-zoom]");
    if (!btn) return;
    const viewer = btn.closest(".svg-viewer");
    const s = state.get(viewer);
    if (!s) return;
    const action = btn.dataset.svgZoom;
    if (action === "in") s.zoom = Math.min(MAX, s.zoom * STEP);
    if (action === "out") s.zoom = Math.max(MIN, s.zoom / STEP);
    if (action === "reset") s.zoom = s.initial;
    apply(viewer);
  });

  document.addEventListener("load", (e) => {
    if (e.target.matches && e.target.matches(".svg-viewer object")) init(e.target);
  }, true);

  window.addEventListener("resize", () =>
    document.querySelectorAll(".svg-viewer").forEach(apply));

  document.querySelectorAll(".svg-viewer object").forEach((o) => {
    if (o.contentDocument && o.contentDocument.documentElement) init(o);
  });
})();
