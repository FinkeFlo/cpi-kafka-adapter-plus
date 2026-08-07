#!/usr/bin/env node
/**
 * Renders the HTML/Tailwind branding sources in this folder to PNG using a
 * headless browser:
 *   - logo.html -> logo.png (full logo with wordmark, used in README/docs)
 *   - icon.html -> icon.png (square standalone icon, for GitHub avatar /
 *                            social preview image)
 *
 * Usage:
 *   node docs/assets/branding/render-logo.js
 *
 * Requires puppeteer (not a project dependency, install on demand):
 *   npm install --no-save puppeteer
 */
const path = require('path');
const puppeteer = require('puppeteer');

const TARGETS = [
  { html: 'logo.html', png: 'logo.png', viewport: { width: 900, height: 300 } },
  { html: 'icon.html', png: 'icon.png', viewport: { width: 350, height: 350 } },
];

(async () => {
  const dir = __dirname;
  const browser = await puppeteer.launch({ headless: 'new' });
  try {
    for (const target of TARGETS) {
      const htmlPath = path.join(dir, target.html);
      const pngPath = path.join(dir, target.png);

      const page = await browser.newPage();
      await page.setViewport({ ...target.viewport, deviceScaleFactor: 3 });
      await page.goto(`file://${htmlPath}`, { waitUntil: 'networkidle0' });

      const element = await page.$('body > div');
      await element.screenshot({ path: pngPath, omitBackground: true });
      await page.close();
      console.log(`Rendered ${target.html} to ${pngPath}`);
    }
  } finally {
    await browser.close();
  }
})();
