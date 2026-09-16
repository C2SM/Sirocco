document$.subscribe(function() {
  const container = document.getElementById('zoom-container');
  const wrapper = document.getElementById('zoom-wrapper');

  if (!container || !wrapper) return;

  let scale = 1;
  const MIN_SCALE = 0.5;
  const MAX_SCALE = 5.0;
  let isHovered = false;

  // Track whether the cursor is over the target container
  container.addEventListener('mouseenter', () => { isHovered = true; });
  container.addEventListener('mouseleave', () => { isHovered = false; });

  // ATTENTION: The listener MUST be on the global window to block browser page zoom
  window.addEventListener('wheel', function(e) {
    // If pinching/ctrl-scrolling AND the cursor is over our specific container
    if (e.ctrlKey && isHovered) {
      e.preventDefault(); // This will now successfully block the whole page zoom!

      // Adjust the zoom calculation
      scale -= e.deltaY / 100;
      scale = Math.max(MIN_SCALE, Math.min(MAX_SCALE, scale));

      // Apply zoom only to the SVG container
      wrapper.style.transform = `scale(${scale})`;
    }
  }, { passive: false }); // Mandatory to allow preventDefault()
});
