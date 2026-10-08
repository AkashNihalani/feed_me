/* Static markup for the Read tab, verbatim from the approved prototype with every id prefixed rd- (other tabs
   share the document). The engine (readerEngine.js) fills it in; readTab.css styles it under .rd.
   The prototype's own header, bottom nav (.appnav) and <main> are not here: the app provides all three
   (the header is ReadHeader). */

/* body-level overlays, rendered into <div class="rd rd-layer" id="rd-layer">: the panel that drops under the
   header, the detail sheet, the desktop hover tip, and the svg gradient the blooms' vignette uses */
export const LAYER_HTML = `<div class="hdrop" id="rd-hdrop"></div>
<div class="ov" id="rd-ov" aria-hidden="true">
  <div class="scrim" id="rd-scrim"></div>
  <section class="sheet" id="rd-sheet" role="dialog" aria-modal="true" aria-label="Details">
    <div class="grab" id="rd-grab"></div>
    <div class="shd" id="rd-shd"><span id="rd-backSlot"></span><span class="mini" id="rd-miniTitle"></span><span class="acts" id="rd-acts"></span></div>
    <div class="obody" id="rd-obody"></div>
  </section>
</div>
<div class="tip" id="rd-tip" aria-hidden="true"></div>
<svg width="0" height="0" style="position:absolute" aria-hidden="true" focusable="false"><defs><radialGradient id="rd-bvig" cx="50%" cy="50%" r="50%"><stop offset="0" stop-color="#000" stop-opacity=".66"/><stop offset=".38" stop-color="#000" stop-opacity=".24"/><stop offset=".78" stop-color="#000" stop-opacity="0"/></radialGradient></defs></svg>`;
