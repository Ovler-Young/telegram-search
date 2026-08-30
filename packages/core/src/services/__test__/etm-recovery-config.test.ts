import { describe, expect, it } from 'vitest'

import {
  createRecoveryInputAuthority,
  parseEtmRecoveryConfig,
} from '../etm-recovery-config'

describe('eTM recovery configuration authority', () => {
  it('parses a minimal SQLite config and absent or empty auxiliaries', () => {
    for (const auxiliary of ['', '\nauxiliary_bots:', '\nauxiliary_bots: []', '\nauxiliary_bots: false']) {
      expect(parseEtmRecoveryConfig(`token: 9007199254740993:main-secret${auxiliary}`)).toEqual({
        mainBotId: '9007199254740993',
        auxiliaryBotIds: [],
        database: { backend: 'sqlite' },
      })
    }
  })

  it('applies exact PostgreSQL defaults and accepts established overrides', () => {
    const credential = 'password-value'
    expect(parseEtmRecoveryConfig(`
token: 9:main
auxiliary_bots:
  - token: 10:aux
database:
  type: postgresql
`)).toEqual({
      mainBotId: '9',
      auxiliaryBotIds: ['10'],
      database: {
        backend: 'postgres',
        database: 'efb_telegram',
        host: 'localhost',
        port: 5432,
        user: 'postgres',
        password: '',
        maxConnections: 8,
        staleTimeout: 300,
        options: '-c timezone=UTC',
      },
    })

    expect(parseEtmRecoveryConfig(`
token: 9:main
database:
  type: postgresql
  database: etm_custom
  host: db.internal
  port: 5544
  user: etm
  password: password-value
  max_connections: 3
  stale_timeout: 123
  options: -c statement_timeout=5000
`).database).toEqual({
      backend: 'postgres',
      database: 'etm_custom',
      host: 'db.internal',
      port: 5544,
      user: 'etm',
      password: credential,
      maxConnections: 3,
      staleTimeout: 123,
      options: '-c statement_timeout=5000',
    })
  })

  it('validates auxiliary shapes, token formats, numeric ranges, and duplicate IDs', () => {
    const invalid = [
      ['[]', 'root'],
      ['token: ""', 'main bot token'],
      ['token: no-colon', 'token format'],
      ['token: 0:secret', 'token format'],
      ['token: 9223372036854775808:secret', 'signed 64-bit'],
      ['token: 9:main\nauxiliary_bots: invalid', 'auxiliary_bots'],
      ['token: 9:main\nauxiliary_bots: [invalid]', 'entry'],
      ['token: 9:main\nauxiliary_bots: [{}]', 'auxiliary bot token'],
      ['token: 9:main\nauxiliary_bots: [{ token: 9:other }]', 'duplicate numeric'],
      ['token: 9:main\nauxiliary_bots: [{ token: 10:a }, { token: 10:b }]', 'duplicate numeric'],
    ] as const
    for (const [contents, error] of invalid)
      expect(() => parseEtmRecoveryConfig(contents)).toThrow(error)
  })

  it('redacts YAML secrets from parser and validation errors', () => {
    const tokenSecret = 'DO_NOT_LEAK_TOKEN'
    const credentialSecret = 'DO_NOT_LEAK_PASSWORD'
    for (const contents of [
      `token: 9:${tokenSecret}\ndatabase: [`,
      `token: 9:${tokenSecret}\ndatabase:\n  type: postgresql\n  password: ${credentialSecret}\n  port: wrong`,
    ]) {
      try {
        parseEtmRecoveryConfig(contents)
        throw new Error('expected failure')
      }
      catch (error) {
        expect(String(error)).not.toContain(tokenSecret)
        expect(String(error)).not.toContain(credentialSecret)
      }
    }
  })

  it('rejects unsupported SSL fields without their values', () => {
    const secret = 'private-certificate-value'
    expect(() => parseEtmRecoveryConfig(`
token: 9:main
database:
  type: postgresql
  sslcert: ${secret}
`)).toThrow('SSL/TLS fields are unsupported: sslcert')
    try {
      parseEtmRecoveryConfig(`token: 9:main\ndatabase: { type: postgresql, sslcert: ${secret} }`)
    }
    catch (error) {
      expect(String(error)).not.toContain(secret)
    }
  })

  it('enforces the database option selected by ETM config', () => {
    const sqlite = parseEtmRecoveryConfig('token: 9:main')
    expect(createRecoveryInputAuthority(sqlite, '/etm/tgdata.db')).toEqual({
      etm: { backend: 'sqlite', path: '/etm/tgdata.db' },
      mainBotId: '9',
      auxiliaryBotIds: [],
    })
    expect(() => createRecoveryInputAuthority(sqlite, '')).toThrow('requires --etm-sqlite')

    const postgres = parseEtmRecoveryConfig('token: 9:main\ndatabase: { type: postgresql }')
    expect(createRecoveryInputAuthority(postgres, '').etm).toMatchObject({ backend: 'postgres' })
    expect(() => createRecoveryInputAuthority(postgres, '/unused')).toThrow('does not accept --etm-sqlite')
  })

  it('selects SQLite unless database.type is exactly postgresql', () => {
    expect(parseEtmRecoveryConfig('token: 9:main\ndatabase: { type: PostgreSQL }').database).toEqual({ backend: 'sqlite' })
  })
})
