import process from 'node:process'

import { mkdtemp, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

import { RECOVERY_REPAIR_FROM_ISO } from '@tg-search/protocol'
import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  emitResult,
  emitStreamResult,
  isCliEntrypoint,
  normalizeRawArgs,
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
  it('does not accept caller-supplied recovery window or bot-role options', async () => {
    for (const option of ['--from', '--to', '--main-bot-username', '--aux-bot-username', '--etm-postgres-url']) {
      const output = captureOutput()
      await runCli(['recovery', 'repair', '--etm-config', '/unused', '--etm-sqlite', '/unused', option, 'value', '--takeout'])
      expect(output.stdoutJson()).toMatchObject({ ok: false })
      vi.restoreAllMocks()
      process.exitCode = originalExitCode
    }
  })

  it('captures the recovery upper bound once before command validation', async () => {
    const output = captureOutput()
    const now = vi.spyOn(Date, 'now').mockReturnValue(Date.parse(RECOVERY_REPAIR_FROM_ISO) + 1)
    await runCli(['recovery', 'repair', '--etm-config', '/unused', '--etm-sqlite', '/unused', '--chunk-size', '0', '--takeout'])
    expect(now).toHaveBeenCalledOnce()
    expect(output.stdoutJson()).toMatchObject({ ok: false })
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

  it('recognizes a symlinked executable as the CLI entrypoint', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'telegram-search-cli-entrypoint-'))
    temporaryDirectories.push(directory)
    const target = join(directory, 'index.mjs')
    const executable = join(directory, 'tg-search')
    await writeFile(target, '')
    await symlink(target, executable)

    expect(isCliEntrypoint(pathToFileURL(target).href, executable)).toBe(true)
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
