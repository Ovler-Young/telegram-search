import type { RecoveryRepairInput } from '@tg-search/protocol'

import { readFile } from 'node:fs/promises'

import { parse } from 'yaml'

const MAX_SIGNED_64 = 9_223_372_036_854_775_807n
const TOKEN_PATTERN = /^([1-9]\d*):.+$/
const SSL_FIELDS = new Set([
  'ssl',
  'tls',
  'sslmode',
  'sslcert',
  'sslkey',
  'sslrootcert',
  'ssl_ca',
  'ssl_cert',
  'ssl_key',
])

type EtmSource = RecoveryRepairInput['etm']

export interface EtmRecoveryAuthority {
  mainBotId: string
  auxiliaryBotIds: string[]
  database: Omit<Extract<EtmSource, { backend: 'postgres' }>, 'backend'> & { backend: 'postgres' }
    | { backend: 'sqlite' }
}

function mapping(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value))
    throw new Error(`${label} must be a mapping`)
  return value as Record<string, unknown>
}

function botIdFromToken(value: unknown, label: string): string {
  if (typeof value !== 'string' || value.length === 0)
    throw new Error(`${label} must be a nonempty string`)
  const match = TOKEN_PATTERN.exec(value)
  if (!match)
    throw new Error(`${label} has an invalid Telegram bot token format`)
  const id = BigInt(match[1])
  if (id > MAX_SIGNED_64)
    throw new Error(`${label} contains a Telegram bot ID outside the signed 64-bit range`)
  return id.toString()
}

function stringField(database: Record<string, unknown>, key: string, fallback: string, allowEmpty = false): string {
  const value = database[key] ?? fallback
  if (typeof value !== 'string' || (!allowEmpty && value.length === 0))
    throw new Error(`ETM database.${key} must be ${allowEmpty ? 'a string' : 'a nonempty string'}`)
  return value
}

function integerField(database: Record<string, unknown>, key: string, fallback: number, minimum: number, maximum = Number.MAX_SAFE_INTEGER): number {
  const value = database[key] ?? fallback
  if (typeof value !== 'number' || !Number.isInteger(value) || value < minimum || value > maximum)
    throw new Error(`ETM database.${key} must be an integer between ${minimum} and ${maximum}`)
  return value
}

export function parseEtmRecoveryConfig(contents: string): EtmRecoveryAuthority {
  let parsed: unknown
  try {
    parsed = parse(contents)
  }
  catch {
    throw new Error('Unable to parse ETM configuration YAML')
  }
  const root = mapping(parsed, 'ETM configuration root')
  const mainBotId = botIdFromToken(root.token, 'ETM main bot token')
  const auxiliaryBotIds: string[] = []
  if (root.auxiliary_bots) {
    if (!Array.isArray(root.auxiliary_bots))
      throw new Error('ETM auxiliary_bots must be an array')
    for (const entry of root.auxiliary_bots) {
      const auxiliary = mapping(entry, 'ETM auxiliary bot entry')
      auxiliaryBotIds.push(botIdFromToken(auxiliary.token, 'ETM auxiliary bot token'))
    }
  }
  const seen = new Set<string>([mainBotId])
  for (const id of auxiliaryBotIds) {
    if (seen.has(id))
      throw new Error('ETM bot configuration contains duplicate numeric bot IDs')
    seen.add(id)
  }

  const database = root.database == null ? {} : mapping(root.database, 'ETM database')
  if (database.type !== 'postgresql')
    return { mainBotId, auxiliaryBotIds, database: { backend: 'sqlite' } }

  const unsupported = Object.keys(database).filter(key => SSL_FIELDS.has(key.toLowerCase())).sort()
  if (unsupported.length)
    throw new Error(`ETM PostgreSQL SSL/TLS fields are unsupported: ${unsupported.join(', ')}`)

  return {
    mainBotId,
    auxiliaryBotIds,
    database: {
      backend: 'postgres',
      database: stringField(database, 'database', 'efb_telegram'),
      host: stringField(database, 'host', 'localhost'),
      port: integerField(database, 'port', 5432, 1, 65_535),
      user: stringField(database, 'user', 'postgres'),
      password: stringField(database, 'password', '', true),
      maxConnections: integerField(database, 'max_connections', 8, 1),
      staleTimeout: integerField(database, 'stale_timeout', 300, 0),
      options: stringField(database, 'options', '-c timezone=UTC', true),
    },
  }
}

export async function readEtmRecoveryConfig(path: string): Promise<EtmRecoveryAuthority> {
  let contents: string
  try {
    contents = await readFile(path, 'utf8')
  }
  catch {
    throw new Error('Unable to read ETM configuration file')
  }
  return parseEtmRecoveryConfig(contents)
}

export function createRecoveryInputAuthority(authority: EtmRecoveryAuthority, sqlitePath: string): Pick<RecoveryRepairInput, 'etm' | 'mainBotId' | 'auxiliaryBotIds'> {
  if (authority.database.backend === 'sqlite') {
    if (!sqlitePath)
      throw new Error('SQLite ETM configuration requires --etm-sqlite')
    return {
      etm: { backend: 'sqlite', path: sqlitePath },
      mainBotId: authority.mainBotId,
      auxiliaryBotIds: authority.auxiliaryBotIds,
    }
  }
  if (sqlitePath)
    throw new Error('PostgreSQL ETM configuration does not accept --etm-sqlite')
  return {
    etm: authority.database,
    mainBotId: authority.mainBotId,
    auxiliaryBotIds: authority.auxiliaryBotIds,
  }
}
