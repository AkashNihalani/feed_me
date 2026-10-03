/* Static markup for the Read tab, verbatim from the approved prototype with every id prefixed rd- (other tabs
   share the document). The engine (readerEngine.ts) fills these in; readTab.css styles them under .rd.
   The prototype's own bottom nav (.appnav) and <main> are not here: the app provides both. */

/* the header row(s), rendered inside the app's header capsule as <div class="rd rd-head"> */
export const HEADER_HTML = `<header class="hdr" id="rd-hdr" aria-label="Read">
  <div class="hrow">
    <div class="hbase"><span class="htitle">Read</span><i class="hdiv" aria-hidden="true"></i><span class="hscope" id="rd-hscope"></span><span class="hmeta" id="rd-hmeta"></span><button type="button" class="hlens" id="rd-hlens" data-drop="lens" aria-expanded="false" aria-label="Layers and filters"></button></div>
    <div class="hread" id="rd-hread" aria-live="polite"></div>
  </div>
  <div class="hcircles" id="rd-hcircles" role="group" aria-label="Accounts"></div>
</header>`;

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
