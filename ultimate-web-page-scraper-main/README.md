# ultimate-web-page-scraper
by AMINE
HTML Grabber Pro — Feature Overview

What it is
A Chrome extension that scrapes any webpage's HTML with computed inline styles baked in — one click, copied to clipboard instantly. Built for workflows where you need clean, self-contained HTML that preserves the exact visual structure of the page.

Scrape Modes
📄 Full Page
Grabs the entire page from <html> to </html>. Every element gets its computed CSS styles written directly into its style="" attribute, so the output is fully self-contained with no dependency on external stylesheets.
🎯 Pick Element
Activates a visual picker overlay on the page. Hover over any element — it highlights in green and shows the tag name. Click to grab just that element and its children. Press ESC to cancel.
⌨️ CSS Selector
Type any CSS selector (.chat-box, #messages, table) and grab exactly that element.

Output Format
All three modes produce clean, properly indented HTML:
html<div style="display:flex;padding:10.5px;background-color:rgb(201, 236, 252);">
    <span style="font-weight:600;margin-right:3.5px;">Land:</span>
    <span style="border-color:rgb(229, 231, 235);">Austria</span>
</div>

Real <tag> / </tag> syntax with proper closing tags
Inline style="property:value;" with colons and semicolons
Computed styles merged with original inline styles
Void elements self-closed (<img />, <br />)
Scripts, styles, meta tags stripped out automatically


Additional Actions (after any grab)

📍 Copy XPath — copies the XPath of the last grabbed element (available after Pick mode)
💾 Save File — downloads the HTML as a .html file, named after the URL with a timestamp


History Tab
Keeps the last 10 grabs stored locally. Each entry shows the mode used, file size, source URL, and timestamp. Per-entry actions: copy to clipboard, save as file, copy XPath, or delete.

Diff Tab
Select any two grabs from history and compare them. Added lines show in green, removed lines in red. Useful for spotting what changed between two page states.

Smart Auto-Detect
On pages with known chat/moderation layouts, the extension automatically detects common sections (customer panel, persona panel, details panel) by background color and structure. Detected sections appear as clickable tags — click one to instantly set it as your CSS selector.

Settings
SettingDefaultDescriptionAuto-detect chat layoutOnHighlights known chat sections on page loadSave to historyOnStores last 10 grabs locallyAuto-copy XPathOffAlso copies XPath when using Pick modeTimestamp filenamesOnAdds date+time to downloaded file names

Keyboard Shortcuts
ShortcutActionCtrl+Shift+GGrab full page instantly (no popup needed)Ctrl+Shift+PLaunch element picker (no popup needed)
(Mac: Cmd instead of Ctrl)

Installation

Download and unzip html-grabber-extension.zip
Open Chrome and go to chrome://extensions
Enable Developer mode (top-right toggle)
Click Load unpacked and select the unzipped folder

To update after changes: go to chrome://extensions and click the ↻ refresh icon on the extension card.
