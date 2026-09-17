// hugly hacky script to find Sirocco keywords in code blocks ...

document.addEventListener("DOMContentLoaded", function () {
  // Find all the code blocks
  // WARNING: This is not yaml specific, it will take all code blocks
  const codeBlocks = document.querySelectorAll(".md-code__content, pre code");

  // Sirocco keywords
  const siroccoKeywords = ["start_date", "stop_date", "period", "cycles", "tasks", "data", "cycling", "inputs", "outputs", "wait_on", "when", "target_cycle", "at", "before", "after", "lag", "date", "parameters"];

  codeBlocks.forEach(codeBlock => {
    // find standard YAML tag tokens inside the block (they all get the nt class)
    const tokens = codeBlock.querySelectorAll(".nt");

    tokens.forEach(token => {
      // If the inner text matches a Sirocco keyword
      if (siroccoKeywords.includes(token.textContent.trim())) {
        // Remove the nt class and replace by the dedicated sirocco-keyword
        // styled in the extra css
        token.classList.remove("nt");
        token.classList.add("sirocco-keyword");
      }

    });

  });

});
