/*
Copyright (C) 2023-2026 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

import { describe, expect, it } from 'vitest'

import { THEME_CHROME_COLORS, THEME_STORAGE_KEYS } from '@/lib/theme-storage'

const indexHtml = readFileSync(
  resolve(import.meta.dirname, '../../../index.html'),
  'utf8'
)
const siteDesignCss = readFileSync(
  resolve(import.meta.dirname, '../../styles/site-design.css'),
  'utf8'
)

// The pre-paint script in index.html is inlined and therefore cannot import the
// theme module: its storage key, allowed values and chrome colours are a
// hand-kept copy. These assertions are the guard against that copy drifting —
// without them a mismatch would surface only as a light flash on a dark system
// or a stale browser-chrome colour, both easy to miss in review.
describe('index.html theme boot script', () => {
  it('reads the same storage key as the provider', () => {
    expect(indexHtml).toContain(THEME_STORAGE_KEYS.mode)
  })

  it('lists every allowed theme value', () => {
    for (const value of ['light', 'dark', 'system']) {
      expect(indexHtml).toContain(`'${value}'`)
    }
  })

  it('inlines the chrome colours declared in the theme module', () => {
    expect(indexHtml).toContain(THEME_CHROME_COLORS.light)
    expect(indexHtml).toContain(THEME_CHROME_COLORS.dark)
  })

  it('keeps the chrome colours in step with the paper palette', () => {
    expect(siteDesignCss).toContain(
      `--site-paper: ${THEME_CHROME_COLORS.light}`
    )
    expect(siteDesignCss).toContain(`--site-paper: ${THEME_CHROME_COLORS.dark}`)
  })
})
