import process from 'node:process'

import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  emitResult,
  emitStreamResult,
  normalizeRawArgs,
  normalizeRecoveryBotUsernames,
  parseRecoveryEtmSource,
  parseRecoveryTimestamp,
  resolveExportOutputPath,
  runCli,
} from './index'
import { readProfileConfig, resolveProfilePaths } from './profile'

const temporaryDirectories: string[] = []
const originalExitCode = process.exitCode

afterEach(async () => {
  vi.restoreAllMocks()
  delete process.env.TG_SEARCH_HOME
  process.exitCode = originalExitCode
  await Promise.all(temporaryDirectories.splice(0).map(path => rm(path, { force: true, recursive: true })))
})

function captureOutput() {
  const stdout = vi.spyOn(process.stdout, 'write').mockImplementation((() => true) as typeof process.stdout.write)
  const stderr = vi.spyOn(process.stderr, 'write').mockImplementation((() => true) as typeof process.stderr.write)
  return {
    stderr,
    stdout,
    stdoutJson: () => JSON.parse(stdout.mock.calls.map(call => String(call[0])).join('')),
  }
}

describe('cLI command boundary', () => {
  it('normalizes and validates recovery bot usernames before opening the runtime', () => {
    expect(normalizeRecoveryBotUsernames(' @Main_Bot ', ['AuxOneBot', '@aux_two_bot'])).toEqual({
      main: 'main_bot',
      auxiliary: ['auxonebot', 'aux_two_bot'],
    })
    expect(() => normalizeRecoveryBotUsernames('not-a-bot', [])).toThrow('Invalid Telegram bot username')
    expect(() => normalizeRecoveryBotUsernames('MainBot', ['@mainbot'])).toThrow('Duplicate Telegram bot username')
  })

  it('requires unambiguous timezone-aware recovery boundaries', () => {
    expect(parseRecoveryTimestamp('2026-01-01T00:00:00+08:00', '--from'))
      .toBe(Date.parse('2025-12-31T16:00:00Z'))
    expect(() => parseRecoveryTimestamp('2026-01-01T00:00:00', '--from')).toThrow('explicit timezone')
    expect(() => parseRecoveryTimestamp('2026-02-30T00:00:00Z', '--to')).toThrow('Invalid timestamp')
  })

  it('requires exactly one ETM database source', () => {
    const sqlitePath = resolve(process.cwd(), 'test-etm.db')
    expect(parseRecoveryEtmSource(sqlitePath, '')).toEqual({ backend: 'sqlite', path: sqlitePath })
    expect(parseRecoveryEtmSource('', 'postgresql://localhost/etm')).toEqual({
      backend: 'postgres',
      url: 'postgresql://localhost/etm',
    })
    expect(() => parseRecoveryEtmSource('', '')).toThrow('exactly one')
    expect(() => parseRecoveryEtmSource(sqlitePath, 'postgresql://localhost/etm')).toThrow('exactly one')
  })

  it('resolves explicit export paths in the invoking process', () => {
    expect(resolveExportOutputPath('./telegram-2026', '/profile/exports')).toBe(
      resolve(process.cwd(), 'telegram-2026'),
    )
    expect(resolveExportOutputPath(undefined, '/profile/exports')).toBe('/profile/exports')
  })

  it('moves a global profile argument to the leaf command', () => {
    expect(normalizeRawArgs(['--profile', 'work', 'messages', 'query', '--json'])).toEqual([
      'messages',
      'query',
      '--profile=work',
    ])
  })

  it('configures the selected named profile instead of default', async () => {
    const home = await mkdtemp(join(tmpdir(), 'telegram-search-cli-'))
    temporaryDirectories.push(home)
    process.env.TG_SEARCH_HOME = home
    const output = captureOutput()

    await runCli(['--profile=work', 'profile', 'configure', '--apiId', '123', '--apiHash', 'secret'])

    await expect(readProfileConfig(resolveProfilePaths('work'))).resolves.toMatchObject({ apiId: '123', apiHash: 'secret' })
    await expect(readProfileConfig(resolveProfilePaths('default'))).resolves.toEqual({})
    expect(output.stdoutJson()).toMatchObject({ ok: true, data: { profile: 'work' }, meta: { profile: 'work', source: 'local' } })
  })

  it('serializes a failed stream as ok=false and exits non-zero', async () => {
    const output = captureOutput()

    await emitStreamResult((async function* () {
      yield {
        type: 'failed',
        error: { code: 'TAKEOUT_FAILED', message: 'Takeout failed', retryable: false },
      }
    })(), { profile: 'work', source: 'telegram' })

    expect(output.stdoutJson()).toMatchObject({
      ok: false,
      error: { code: 'TAKEOUT_FAILED' },
      meta: { profile: 'work', source: 'telegram' },
    })
    expect(process.exitCode).toBe(1)
  })

  it('keeps RPC errors structured and exits non-zero', () => {
    const output = captureOutput()

    emitResult({
      ok: false,
      error: { code: 'INVALID_ARGUMENT', message: 'limit must be positive', retryable: false },
    }, { profile: 'work', source: 'local' })

    expect(output.stdoutJson()).toMatchObject({
      ok: false,
      error: { code: 'INVALID_ARGUMENT' },
      meta: { profile: 'work', source: 'local' },
    })
    expect(process.exitCode).toBe(1)
  })

  it('rejects a stream that ends without a terminal update', async () => {
    captureOutput()

    await expect(emitStreamResult((async function* () {
      yield { type: 'progress' }
    })(), { profile: 'work', source: 'local' })).rejects.toThrow('without a terminal')
  })
})
