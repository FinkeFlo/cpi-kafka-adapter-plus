#!/usr/bin/env node
/**
 * Renders docs/assets/branding/logo.html to logo.png using a headless browser.
 *
 * Usage:
 *   node docs/assets/branding/render-logo.js
 *
 * Requires puppeteer (not a project dependency, install on demand):
 *   npm install --no-save puppeteer
 */
const path = require('path');
const puppeteer = require('puppeteer');

(async () => {
  const dir = __dirname;
  const htmlPath = path.join(dir, 'logo.html');
  const pngPath = path.join(dir, 'logo.png');

  const browser = await puppeteer.launch({ headless: 'new' });
  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 900, height: 300, deviceScaleFactor: 3 });
    await page.goto(`file://${htmlPath}`, { waitUntil: 'networkidle0' });

    const element = await page.$('body > div');
    await element.screenshot({ path: pngPath, omitBackground: true });
    console.log(`Logo rendered to ${pngPath}`);
  } finally {
    await browser.close();
  }
})();
