import type { Logger } from '@guiiai/logg'

import type { CoreContext } from '../../context'
import type { RepairCandidate } from '../recovery-repair'
import type { TakeoutService } from '../takeout'

import process from 'node:process'

import { Buffer } from 'node:buffer'
import { chmod, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

import bigInt from 'big-integer'

import { RECOVERY_REPAIR_FROM_ISO } from '@tg-search/protocol'
import { EventEmitter } from 'eventemitter3'
import { Pool } from 'pg'
import { Api } from 'telegram'
import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  createRecoveryRepairService,
  insertRepairCandidates,
  inspectEtm,
  normalizeBindings,
  parseEtmGroupId,
  postgresPoolConfig,
  readHistoricalBindingHits,
  readHistoricalGroupIds,
  readInitialPresences,
} from '../recovery-repair'

const temporaryDirectories: string[] = []

function logger(): Logger {
  const value: Record<string, unknown> = {}
  const chain = () => value
  for (const method of ['withContext', 'withFields', 'withError', 'withLogLevel', 'withLogLevelString', 'useGlobalConfig'])
    value[method] = chain
  for (const method of ['debug', 'verbose', 'log', 'warn', 'error'])
    value[method] = () => {}
  return value as unknown as Logger
}

function createEtmDatabase(path: string) {
  const database = new DatabaseSync(path)
  database.exec(`
    CREATE TABLE topicassoc (id INTEGER PRIMARY KEY, topic_chat_id TEXT, message_thread_id TEXT, slave_uid TEXT);
    CREATE TABLE slavechatinfo (
      id INTEGER PRIMARY KEY,
      slave_channel_id TEXT NOT NULL,
      slave_chat_uid TEXT NOT NULL,
      slave_chat_group_id TEXT,
      slave_chat_name TEXT
    );
    CREATE TABLE msglog (
      master_msg_id TEXT PRIMARY KEY,
      master_msg_id_alt TEXT,
      slave_message_id TEXT NOT NULL,
      text TEXT NOT NULL,
      slave_origin_uid TEXT NOT NULL,
      slave_origin_display_name TEXT,
      slave_member_uid TEXT,
      slave_member_display_name TEXT,
      media_type TEXT,
      mime TEXT,
      file_id TEXT,
      file_unique_id TEXT,
      msg_type TEXT NOT NULL,
      pickle BLOB,
      sent_to TEXT NOT NULL,
      sender_bot_id TEXT,
      time DATETIME
    );
  `)
  return database
}

function insertSlaveName(database: DatabaseSync, id: number, slaveUid: string, name: string | null) {
  const [slaveChannelId, slaveChatUid, slaveChatGroupId = null] = slaveUid.split(' ')
  database.prepare(`INSERT INTO slavechatinfo (
    id, slave_channel_id, slave_chat_uid, slave_chat_group_id, slave_chat_name
  ) VALUES (?, ?, ?, ?, ?)`).run(id, slaveChannelId, slaveChatUid, slaveChatGroupId, name)
}

function insertLegacySlaveName(database: DatabaseSync, masterId: string, slaveUid: string, name: string) {
  database.prepare(`INSERT INTO msglog (
    master_msg_id, master_msg_id_alt, slave_message_id, text, slave_origin_uid,
    slave_origin_display_name, slave_member_uid, media_type, msg_type, sent_to, time
  ) VALUES (?, NULL, 'live', 'live', ?, ?, 'slave.module member', 'Text', 'Text', 'blueset.telegram', '2026-01-01')`)
    .run(masterId, slaveUid, name)
}

function insertExisting(database: DatabaseSync, primary: string, alternate: string | null, senderBotId: string | null = null) {
  database.prepare(`INSERT INTO msglog (
    master_msg_id, master_msg_id_alt, slave_message_id, text, slave_origin_uid,
    slave_member_uid, media_type, msg_type, sent_to, sender_bot_id, time
  ) VALUES (?, ?, 'live', 'live', 'slave.module chat', 'slave.module member', 'Text', 'Text', 'blueset.telegram', ?, '2026-01-01')`)
    .run(primary, alternate, senderBotId)
}

afterEach(async () => {
  vi.restoreAllMocks()
  await Promise.all(temporaryDirectories.splice(0).map(path => rm(path, { recursive: true, force: true })))
})

async function collectUpdates<T>(generator: AsyncGenerator<T>): Promise<T[]> {
  const updates = []
  for await (const update of generator)
    updates.push(update)
  return updates
}

describe('eTM discovery and bot identity', () => {
  it('normalizes marked group IDs and rejects conflicting or unusable bindings', () => {
    expect(parseEtmGroupId('-1000000000042')).toEqual({
      topicChatId: '-1000000000042',
      sourceChatId: '42',
      expectedPeer: 'channel',
    })
    expect(() => normalizeBindings([
      { topic_chat_id: '-42', message_thread_id: '7', slave_uid: 'slave.module chat-a' },
      { topic_chat_id: '-42', message_thread_id: '7', slave_uid: 'slave.module chat-b' },
    ])).toThrow('Conflicting')
    expect(() => normalizeBindings([
      { topic_chat_id: '-42', message_thread_id: '7', slave_uid: 'not-a-bound-chat' },
    ])).toThrow('Unusable')
  })

  it('deduplicates exact TopicAssoc rows and permits one slave UID on multiple topics', () => {
    expect(normalizeBindings([
      { topic_chat_id: '-1000000000042', message_thread_id: '11', slave_uid: 'slave.module chat-a' },
      { topic_chat_id: '-1000000000042', message_thread_id: '10', slave_uid: 'slave.module chat-a' },
      { topic_chat_id: '-1000000000042', message_thread_id: '10', slave_uid: 'slave.module chat-a' },
    ])).toEqual([
      { topicChatId: '-1000000000042', messageThreadId: '10', slaveUid: 'slave.module chat-a', slaveModule: 'slave.module' },
      { topicChatId: '-1000000000042', messageThreadId: '11', slaveUid: 'slave.module chat-a', slaveModule: 'slave.module' },
    ])
  })

  it('validates a real SQLite schema and checks both MsgLog identity columns', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const path = join(directory, 'etm.db')
    const database = createEtmDatabase(path)
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(1, '-1000000000042', '10', 'slave.module chat-a')
    insertExisting(database, '-1000000000042.11', '-1000000000042.12')
    database.close()

    await expect(inspectEtm({ backend: 'sqlite', path })).resolves.toEqual({
      bindings: [{ topicChatId: '-1000000000042', messageThreadId: '10', slaveUid: 'slave.module chat-a', slaveModule: 'slave.module' }],
      slaveNames: new Map([['slave.module chat-a', {
        slaveUid: 'slave.module chat-a',
        slaveName: 'slave.module chat-a',
        nameSource: 'slave_uid',
      }]]),
    })
    await expect(readInitialPresences({ backend: 'sqlite', path }, [
      '-1000000000042.11',
      '-1000000000042.12',
      '-1000000000042.13',
    ])).resolves.toEqual(new Map([
      ['-1000000000042.11', 'primary'],
      ['-1000000000042.12', 'alternate'],
      ['-1000000000042.13', 'missing'],
    ]))
  })

  it('discovers historical groups and exact reverse bindings from both MsgLog master ID columns', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const path = join(directory, 'etm.db')
    const database = createEtmDatabase(path)
    insertExisting(database, '-1000000000042.11', '-1000000000043.12')
    insertExisting(database, 'legacy.invalid', null)
    database.close()

    await expect(readHistoricalGroupIds({ backend: 'sqlite', path })).resolves.toEqual([
      '-1000000000043',
      '-1000000000042',
    ])
    await expect(readHistoricalBindingHits({ backend: 'sqlite', path }, [
      '-1000000000042.11',
      '-1000000000043.12',
      '-1000000000042.99',
    ])).resolves.toEqual(new Map([
      ['-1000000000042.11', ['slave.module chat']],
      ['-1000000000043.12', ['slave.module chat']],
    ]))
  })

  it('uses PostgreSQL exact master-ID lookup and server-side historical group extraction', async () => {
    const statements: Array<{ sql: string, values?: unknown[] }> = []
    const query = vi.fn(async (sqlValue: unknown, values?: unknown[]) => {
      const sql = String(sqlValue)
      statements.push({ sql, values })
      if (sql.includes('substring(identity'))
        return { rows: [{ topic_chat_id: '-1000000000042' }], rowCount: 1 }
      return {
        rows: [{
          master_msg_id: '-1000000000042.11',
          master_msg_id_alt: '-1000000000042.12',
          slave_origin_uid: 'slave.module chat-a',
        }],
        rowCount: 1,
      }
    })
    vi.spyOn(Pool.prototype, 'connect').mockResolvedValue({ query, release: vi.fn() } as never)
    vi.spyOn(Pool.prototype, 'end').mockResolvedValue()
    const source = {
      backend: 'postgres' as const,
      database: 'etm',
      host: 'db',
      port: 5432,
      user: 'etm',
      password: process.env.TEST_DATABASE_PASSWORD ?? '',
      maxConnections: 2,
      staleTimeout: 0,
      options: '',
    }

    await expect(readHistoricalGroupIds(source)).resolves.toEqual(['-1000000000042'])
    await expect(readHistoricalBindingHits(source, ['-1000000000042.12'])).resolves.toEqual(new Map([
      ['-1000000000042.12', ['slave.module chat-a']],
    ]))
    expect(statements[0].sql).toContain('identity ~ \'^-[0-9]+\\.[1-9][0-9]*$\'')
    expect(statements[1]).toMatchObject({
      sql: expect.stringContaining('master_msg_id_alt = ANY($1::text[])'),
      values: [['-1000000000042.12']],
    })
  })

  it('resolves SQLite slave display names from SlaveChatInfo before MsgLog fallback', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const path = join(directory, 'etm.db')
    const database = createEtmDatabase(path)
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(1, '-1000000000042', '10', 'slave.module chat-a')
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(2, '-1000000000042', '11', 'slave.module chat-b')
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(3, '-1000000000042', '12', 'slave.module chat-c room-1')
    insertSlaveName(database, 1, 'slave.module chat-a', 'Old Alpha')
    insertSlaveName(database, 2, 'slave.module chat-a', '')
    insertSlaveName(database, 3, 'slave.module chat-a', 'Current Alpha')
    insertLegacySlaveName(database, '-1000000000042.1', 'slave.module chat-b', 'Legacy Beta')
    database.close()

    const inspected = await inspectEtm({ backend: 'sqlite', path })
    expect(inspected.slaveNames).toEqual(new Map([
      ['slave.module chat-a', {
        slaveUid: 'slave.module chat-a',
        slaveName: 'Current Alpha',
        nameSource: 'slavechatinfo.slave_chat_name',
      }],
      ['slave.module chat-b', {
        slaveUid: 'slave.module chat-b',
        slaveName: 'Legacy Beta',
        nameSource: 'msglog.slave_origin_display_name',
      }],
      ['slave.module chat-c room-1', {
        slaveUid: 'slave.module chat-c room-1',
        slaveName: 'slave.module chat-c room-1',
        nameSource: 'slave_uid',
      }],
    ]))
  })

  it('resolves PostgreSQL slave display names from the inspected ETM schema', async () => {
    const columns = {
      topicassoc: ['topic_chat_id', 'message_thread_id', 'slave_uid'],
      slavechatinfo: ['id', 'slave_channel_id', 'slave_chat_uid', 'slave_chat_group_id', 'slave_chat_name'],
      msglog: [
        'master_msg_id',
        'master_msg_id_alt',
        'slave_message_id',
        'text',
        'slave_origin_uid',
        'slave_origin_display_name',
        'slave_member_uid',
        'slave_member_display_name',
        'media_type',
        'mime',
        'file_id',
        'file_unique_id',
        'msg_type',
        'pickle',
        'sent_to',
        'sender_bot_id',
        'time',
      ],
    }
    const query = vi.fn(async (query: unknown) => {
      const sql = String(query)
      if (sql.startsWith('SELECT table_name, column_name')) {
        return {
          rows: Object.entries(columns).flatMap(([table_name, names]) =>
            names.map(column_name => ({ table_name, column_name }))),
        }
      }
      if (sql.startsWith('SELECT topic_chat_id')) {
        return {
          rows: [{ topic_chat_id: '-1000000000042', message_thread_id: '10', slave_uid: 'slave.module chat-a' }],
        }
      }
      if (sql.startsWith('SELECT slave_chat_name'))
        return { rows: [{ slave_chat_name: 'Postgres Alpha' }] }
      if (sql.startsWith('SELECT slave_origin_display_name'))
        return { rows: [] }
      return { rows: [], rowCount: null }
    })
    vi.spyOn(Pool.prototype, 'connect').mockResolvedValue({ query, release: vi.fn() } as never)
    vi.spyOn(Pool.prototype, 'end').mockResolvedValue()
    const password = ['test', 'credential'].join('-')

    const inspected = await inspectEtm({
      backend: 'postgres',
      database: 'custom',
      host: 'db.internal',
      port: 5544,
      user: 'etm',
      password,
      maxConnections: 3,
      staleTimeout: 999,
      options: '-c timezone=UTC',
    })
    expect(inspected.bindings).toMatchObject([{ topicChatId: '-1000000000042', messageThreadId: '10', slaveUid: 'slave.module chat-a' }])
    expect(inspected.slaveNames).toEqual(new Map([['slave.module chat-a', {
      slaveUid: 'slave.module chat-a',
      slaveName: 'Postgres Alpha',
      nameSource: 'slavechatinfo.slave_chat_name',
    }]]))
  })

  it.runIf(Boolean(process.env.RECOVERY_REPAIR_POSTGRES_URL))('validates the configured real PostgreSQL ETM schema', async () => {
    await expect(
      inspectEtm({
        backend: 'postgres',
        database: 'efb_telegram',
        host: 'localhost',
        port: 5432,
        user: 'postgres',
        password: '',
        maxConnections: 8,
        staleTimeout: 300,
        options: '-c timezone=UTC',
      }),
    ).resolves.toMatchObject({ bindings: expect.any(Array) })
  })
})

describe('bounded recovery repair', () => {
  it('uses the fixed incident cutoff and rejects a clock at that boundary', async () => {
    expect(RECOVERY_REPAIR_FROM_ISO).toBe('2026-07-13T18:22:03Z')
    const inspect = vi.fn()
    const service = createRecoveryRepairService({
      context: { emitter: new EventEmitter(), getClient: vi.fn() } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer: vi.fn() },
      takeoutService: { takeoutMessages: vi.fn() },
      inspect,
    })
    const iterator = service({
      etm: { backend: 'sqlite', path: '/unused' },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: Date.parse(RECOVERY_REPAIR_FROM_ISO),
      chunkSize: 10,
      outputFile: null,
      takeout: true,
    })
    await expect(iterator.next()).rejects.toThrow(RECOVERY_REPAIR_FROM_ISO)
    expect(inspect).not.toHaveBeenCalled()
  })

  it('inserts bot text and metadata without persisting media, filters other records, and reruns idempotently', async () => {
    const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
    const toMs = (fromSeconds + 200) * 1000
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const path = join(directory, 'etm.db')
    const output = join(directory, 'report.jsonl')
    const database = createEtmDatabase(path)
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(1, '-1000000000042', '10', 'slave.module chat-a')
    insertExisting(database, '-1000000000042.100', null)
    insertExisting(database, '-1000000000042.199', '-1000000000042.101')
    insertExisting(database, 'legacy.aux', null, '10')
    database.close()

    const main = new Api.User({ id: bigInt(9), firstName: 'Main', username: 'main_bot', bot: true })
    const auxiliary = new Api.User({ id: bigInt(10), firstName: 'Aux', username: 'aux_bot', bot: true })
    const unknownBot = new Api.User({ id: bigInt(12), firstName: 'Unknown', username: 'unknown_bot', bot: true })
    const human = new Api.User({ id: bigInt(11), firstName: 'Human', bot: false })
    const group = new Api.Channel({
      id: bigInt(42),
      accessHash: bigInt(1),
      title: 'Bound',
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const client = {
      getEntity: vi.fn(async (peer: unknown) => {
        const id = peer instanceof Api.PeerUser ? peer.userId.toString() : undefined
        return id === '9' ? main : id === '10' ? auxiliary : id === '12' ? unknownBot : id === '11' ? human : group
      }),
    }
    const context = { emitter: new EventEmitter(), getClient: () => client } as unknown as CoreContext
    const message = (id: number, sender: number, text: string, top?: number, reply?: number, media?: Api.TypeMessageMedia) => new Api.Message({
      id,
      peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
      fromId: new Api.PeerUser({ userId: bigInt(sender) }),
      date: id === 99 ? fromSeconds - 1 : fromSeconds + id,
      message: text,
      replyTo: top || reply ? new Api.MessageReplyHeader({ replyToMsgId: reply ?? top!, replyToTopId: top }) : undefined,
      media,
    })
    const messages = [
      message(99, 9, 'outside', 10),
      message(100, 9, 'present primary', 10),
      message(101, 9, 'present alternate', 10),
      message(102, 9, 'main text', 10, undefined, new Api.MessageMediaPhoto({
        photo: new Api.Photo({
          id: bigInt(102),
          accessHash: bigInt(1),
          fileReference: Buffer.from([]),
          date: 0,
          sizes: [],
          dcId: 1,
        }),
      })),
      message(103, 10, 'aux text', undefined, 10),
      message(104, 11, 'human text', 10),
      message(107, 12, 'unknown bot text', 10),
      message(105, 9, 'wrong topic', 99),
      message(106, 9, '', 10),
      message(200, 9, 'outside', 10),
    ]
    const takeoutMessages = vi.fn(async function* (_chatId: string, options: Parameters<TakeoutService['takeoutMessages']>[1]) {
      if (options.replyTo !== undefined) {
        expect(options).toMatchObject({ reverse: true, skipMedia: true, takeoutConsent: true })
        return
      }
      expect(options.startTime).toBe(Date.parse(RECOVERY_REPAIR_FROM_ISO))
      expect(options.endTime).toBe(toMs - 1)
      expect(options.skipMedia).toBe(true)
      yield* messages
    })
    const service = createRecoveryRepairService({
      context,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: { takeoutMessages },
    })
    const input = {
      etm: { backend: 'sqlite' as const, path },
      mainBotId: '9',
      auxiliaryBotIds: ['10'],
      startedAtMs: toMs,
      chunkSize: 1,
      outputFile: output,
      takeout: true,
    }

    const first = (await collectUpdates(service(input))).at(-1)
    expect(first).toMatchObject({
      type: 'completed',
      summary: {
        window: {
          from: RECOVERY_REPAIR_FROM_ISO,
          to: new Date(toMs).toISOString(),
          semantics: '[from,to)',
        },
        counts: {
          'present-primary': 1,
          'present-alt': 1,
          'inserted': 2,
          'unbound-topic': 1,
          'human-or-unconfigured-sender': 2,
          'service-deleted-unusable': 1,
          'concurrent': 0,
          'conflicts': 0,
          'errors': 0,
        },
      },
    })
    const report = await readFile(output, 'utf8')
    expect(report).not.toContain('main text')
    expect(report).not.toContain('@main_bot')

    const check = new DatabaseSync(path, { readOnly: true })
    const rows = check.prepare(`SELECT master_msg_id, master_msg_id_alt, slave_message_id, text, slave_origin_uid,
      slave_member_uid, media_type, mime, file_id, file_unique_id, msg_type, sent_to, sender_bot_id
      FROM msglog WHERE master_msg_id IN (?, ?) ORDER BY master_msg_id`).all('-1000000000042.102', '-1000000000042.103')
    check.close()
    expect(rows).toEqual([
      {
        master_msg_id: '-1000000000042.102',
        master_msg_id_alt: null,
        slave_message_id: 'mtproto-backfill:-1000000000042.102',
        text: 'main text',
        slave_origin_uid: 'slave.module chat-a',
        slave_member_uid: 'slave.module __self__',
        media_type: 'Text',
        mime: null,
        file_id: null,
        file_unique_id: null,
        msg_type: 'Text',
        sent_to: 'blueset.telegram',
        sender_bot_id: null,
      },
      {
        master_msg_id: '-1000000000042.103',
        master_msg_id_alt: null,
        slave_message_id: 'mtproto-backfill:-1000000000042.103',
        text: 'aux text',
        slave_origin_uid: 'slave.module chat-a',
        slave_member_uid: 'slave.module __self__',
        media_type: 'Text',
        mime: null,
        file_id: null,
        file_unique_id: null,
        msg_type: 'Text',
        sent_to: 'blueset.telegram',
        sender_bot_id: '10',
      },
    ])

    const second = (await collectUpdates(service(input))).at(-1)
    expect(second).toMatchObject({ summary: { counts: { 'present-primary': 3, 'present-alt': 1, 'inserted': 0 } } })
  })

  it('reports mapped counts for each corresponding ETM slave without message bodies', async () => {
    const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
    const toMs = (fromSeconds + 200) * 1000
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const output = join(directory, 'report.jsonl')
    const group = new Api.Channel({
      id: bigInt(42),
      accessHash: bigInt(1),
      title: 'Bound',
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const bindings = normalizeBindings([
      { topic_chat_id: '-1000000000042', message_thread_id: '10', slave_uid: 'slave.module chat-a' },
      { topic_chat_id: '-1000000000042', message_thread_id: '11', slave_uid: 'slave.module chat-a' },
      { topic_chat_id: '-1000000000042', message_thread_id: '12', slave_uid: 'slave.module chat-b' },
    ])
    const slaveNames = new Map([
      ['slave.module chat-a', {
        slaveUid: 'slave.module chat-a',
        slaveName: 'Alpha Room',
        nameSource: 'slavechatinfo.slave_chat_name' as const,
      }],
      ['slave.module chat-b', {
        slaveUid: 'slave.module chat-b',
        slaveName: 'Beta Room',
        nameSource: 'slavechatinfo.slave_chat_name' as const,
      }],
    ])
    const message = (id: number, sender: number, text: string, topicId: number) => new Api.Message({
      id,
      peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
      fromId: new Api.PeerUser({ userId: bigInt(sender) }),
      date: fromSeconds + id,
      message: text,
      replyTo: new Api.MessageReplyHeader({ replyToMsgId: topicId }),
    })
    const service = createRecoveryRepairService({
      context: {
        emitter: new EventEmitter(),
        getClient: () => ({ getEntity: vi.fn(async () => group) }),
      } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: {
        async* takeoutMessages() {
          yield message(100, 9, 'alpha present primary body', 10)
          yield message(101, 9, 'alpha present alternate body', 11)
          yield message(102, 9, 'alpha inserted body', 10)
          yield message(103, 11, 'alpha human body', 11)
          yield message(104, 10, 'beta inserted body', 12)
          yield message(105, 11, 'beta human body', 12)
          yield message(106, 9, 'unbound body', 99)
          yield message(107, 9, '', 10)
        },
      },
      inspect: vi.fn(async () => ({ bindings, slaveNames })),
      presences: vi.fn(async () => new Map([
        ['-1000000000042.100', 'primary' as const],
        ['-1000000000042.101', 'alternate' as const],
      ])),
      insert: vi.fn(async () => ({
        inserted: 2,
        concurrent: 0,
        conflicts: 0,
        errors: 0,
        statuses: new Map([
          ['-1000000000042.102', 'inserted' as const],
          ['-1000000000042.104', 'inserted' as const],
        ]),
      })),
    })

    const updates = await collectUpdates(service({
      etm: { backend: 'sqlite', path: '/unused' },
      mainBotId: '9',
      auxiliaryBotIds: ['10'],
      startedAtMs: toMs,
      chunkSize: 10,
      outputFile: output,
      takeout: true,
    }))
    const summaries = updates.filter(update => update.type === 'slave-summary')
    expect(updates.slice(0, 6).map(update => update.type === 'recovery-stage'
      ? `${update.type}:${update.stage}:${update.status}`
      : update.type)).toEqual([
      'started',
      'recovery-stage:etm-inspection:started',
      'recovery-stage:etm-inspection:completed',
      'recovery-stage:historical-group-discovery:started',
      'recovery-stage:historical-group-discovery:completed',
      'group-start',
    ])
    expect(updates[5]).toMatchObject({
      topicChatId: '-1000000000042',
      sourceChatId: '42',
      source: 'topic-assoc',
      bindingCount: 3,
    })
    expect(summaries).toMatchObject([
      {
        type: 'slave-summary',
        taskId: expect.any(String),
        version: 2,
        topicChatId: '-1000000000042',
        slaveUid: 'slave.module chat-a',
        slaveName: 'Alpha Room',
        nameSource: 'slavechatinfo.slave_chat_name',
        counts: {
          mappedExamined: 5,
          eligible: 3,
          presentPrimary: 1,
          presentAlt: 1,
          inserted: 1,
          concurrent: 0,
          conflicts: 0,
          errors: 0,
          skipped: {
            'human-or-unconfigured-sender': 1,
            'service-deleted-unusable': 1,
          },
        },
      },
      {
        type: 'slave-summary',
        taskId: expect.any(String),
        version: 2,
        topicChatId: '-1000000000042',
        slaveUid: 'slave.module chat-b',
        slaveName: 'Beta Room',
        nameSource: 'slavechatinfo.slave_chat_name',
        counts: {
          mappedExamined: 2,
          eligible: 1,
          presentPrimary: 0,
          presentAlt: 0,
          inserted: 1,
          concurrent: 0,
          conflicts: 0,
          errors: 0,
          skipped: {
            'human-or-unconfigured-sender': 1,
            'service-deleted-unusable': 0,
          },
        },
      },
    ])
    expect(updates.find(update => update.type === 'group-complete')).toMatchObject({
      slaveCount: 2,
      mappedExamined: 7,
    })
    expect(updates.at(-1)).toMatchObject({
      summary: {
        counts: {
          'present-primary': 1,
          'present-alt': 1,
          'inserted': 2,
          'unbound-topic': 1,
          'human-or-unconfigured-sender': 2,
          'service-deleted-unusable': 1,
        },
        examined: 8,
      },
    })

    const report = await readFile(output, 'utf8')
    const reportLines = report.trim().split('\n').map(line => JSON.parse(line) as Record<string, unknown>)
    expect(reportLines.map(line => line.type)).toEqual(['run-start', 'group-start', 'slave-summary', 'slave-summary', 'group-complete', 'run-complete'])
    expect(reportLines[4]).toMatchObject({
      type: 'group-complete',
      slaveCount: 2,
      mappedExamined: 7,
      examined: 8,
      candidates: [
        expect.objectContaining({ identity: '-1000000000042.100', status: 'present-primary' }),
        expect.objectContaining({ identity: '-1000000000042.101', status: 'present-alt' }),
        expect.objectContaining({ identity: '-1000000000042.102', status: 'repair-attempted' }),
        expect.objectContaining({ identity: '-1000000000042.104', status: 'repair-attempted' }),
      ],
    })
    expect(report).not.toContain('alpha inserted body')
    expect(report).not.toContain('beta inserted body')
    expect(report).not.toContain('alpha human body')
  })

  it('reports bounded acquisition counts without changing completed-group progress semantics', async () => {
    const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const output = join(directory, 'report.jsonl')
    const binding = normalizeBindings([
      { topic_chat_id: '-1000000000042', message_thread_id: '10', slave_uid: 'slave.module chat-a' },
    ])[0]
    const group = new Api.Channel({
      id: bigInt(42),
      accessHash: bigInt(1),
      title: 'Large',
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const service = createRecoveryRepairService({
      context: { emitter: new EventEmitter(), getClient: () => ({ getEntity: vi.fn(async () => group) }) } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: {
        async* takeoutMessages() {
          for (let id = 1; id <= 10_001; id++) {
            yield new Api.Message({
              id,
              peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
              date: fromSeconds + id,
              message: '',
              replyTo: new Api.MessageReplyHeader({ replyToMsgId: 10 }),
            })
          }
        },
      },
      inspect: vi.fn(async () => ({ bindings: [binding] })),
      presences: vi.fn(async () => new Map()),
      insert: vi.fn(async () => ({ inserted: 0, concurrent: 0, conflicts: 0, errors: 0, statuses: new Map() })),
    })

    const updates = await collectUpdates(service({
      etm: { backend: 'sqlite', path: '/unused' },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: (fromSeconds + 20_000) * 1000,
      chunkSize: 10,
      outputFile: output,
      takeout: true,
    }))

    expect(updates.filter(update => update.type === 'group-acquisition-progress')).toEqual([
      expect.objectContaining({ acquired: 10_000, topicChatId: '-1000000000042', sourceChatId: '42' }),
    ])
    expect(updates.filter(update => update.type === 'progress')).toEqual([
      expect.objectContaining({ examined: 10_001 }),
    ])
    const report = (await readFile(output, 'utf8')).trim().split('\n').map(line => JSON.parse(line) as Record<string, unknown>)
    expect(report.filter(event => event.type === 'group-acquisition-progress')).toEqual([
      expect.objectContaining({ acquired: 10_000, topicChatId: '-1000000000042', sourceChatId: '42' }),
    ])
  })

  it('emits acquisition heartbeats while awaiting one Telegram iterator request', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] })
    try {
      const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
      const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
      temporaryDirectories.push(directory)
      const output = join(directory, 'report.jsonl')
      const binding = normalizeBindings([
        { topic_chat_id: '-1000000000042', message_thread_id: '10', slave_uid: 'slave.module chat-a' },
      ])[0]
      const group = new Api.Channel({
        id: bigInt(42),
        accessHash: bigInt(1),
        title: 'Delayed',
        photo: new Api.ChatPhotoEmpty(),
        date: 0,
        megagroup: true,
      })
      const delayed = (async function* () {
        await new Promise(resolve => setTimeout(resolve, 90_000))
        yield new Api.Message({
          id: 500,
          peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
          date: fromSeconds + 1,
          message: '',
          replyTo: new Api.MessageReplyHeader({ replyToMsgId: 10 }),
        })
      })()
      const telegramNext = vi.spyOn(delayed, 'next')
      const service = createRecoveryRepairService({
        context: { emitter: new EventEmitter(), getClient: () => ({ getEntity: vi.fn(async () => group) }) } as unknown as CoreContext,
        logger: logger(),
        entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
        takeoutService: { takeoutMessages: vi.fn(() => delayed) },
        inspect: vi.fn(async () => ({ bindings: [binding] })),
        presences: vi.fn(async () => new Map()),
        insert: vi.fn(async () => ({ inserted: 0, concurrent: 0, conflicts: 0, errors: 0, statuses: new Map() })),
      })
      const stream = service({
        etm: { backend: 'sqlite', path: '/unused' },
        mainBotId: '9',
        auxiliaryBotIds: [],
        startedAtMs: (fromSeconds + 10) * 1000,
        chunkSize: 10,
        outputFile: output,
        takeout: true,
      })
      for (let index = 0; index < 6; index++)
        await stream.next()

      const pendingHeartbeat = stream.next()
      while (telegramNext.mock.calls.length === 0)
        await new Promise(resolve => setImmediate(resolve))
      await vi.advanceTimersByTimeAsync(60_000)
      await expect(pendingHeartbeat).resolves.toMatchObject({
        value: {
          type: 'group-acquisition-heartbeat',
          acquired: 0,
          elapsedMs: 60_000,
          idleMs: 60_000,
        },
      })
      expect(telegramNext).toHaveBeenCalledOnce()

      const pendingCompletion = stream.next()
      await vi.advanceTimersByTimeAsync(0)
      await vi.advanceTimersByTimeAsync(30_000)
      await pendingCompletion
      expect(telegramNext).toHaveBeenCalledTimes(2)
      let done = false
      while (!done)
        done = (await stream.next()).done === true
      const report = await readFile(output, 'utf8')
      expect(report).toContain('"type":"group-acquisition-heartbeat"')
    }
    finally {
      vi.useRealTimers()
    }
  })

  it('repairs an unassociated historical topic after paging past the first ten reverse-lookup anchors', async () => {
    const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const output = join(directory, 'report.jsonl')
    const group = new Api.Channel({
      id: bigInt(42),
      accessHash: bigInt(1),
      title: 'Historical',
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const reverseLookup = vi.fn(async (_source: unknown, identities: string[]) => identities.includes('-1000000000042.11')
      ? new Map([['-1000000000042.11', ['slave.module chat-a']]])
      : new Map())
    const insert = vi.fn(async (_source: unknown, candidates: RepairCandidate[]) => ({
      inserted: candidates.length,
      concurrent: 0,
      conflicts: 0,
      errors: 0,
      statuses: new Map(candidates.map(candidate => [candidate.identity, 'inserted' as const])),
    }))
    const client = { getEntity: vi.fn(async () => group) }
    const takeoutMessages = vi.fn(async function* (_chatId: string, options: Parameters<TakeoutService['takeoutMessages']>[1]) {
      if (options.replyTo === 77) {
        expect(options).toMatchObject({ reverse: true, skipMedia: true, takeoutConsent: true })
        for (let id = 1; id <= 11; id++) {
          yield new Api.Message({
            id,
            peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
            fromId: new Api.PeerUser({ userId: bigInt(9) }),
            date: fromSeconds - 100 + id,
            message: `anchor ${id}`,
            replyTo: new Api.MessageReplyHeader({ replyToTopId: 77, replyToMsgId: 77 }),
          })
        }
        return
      }
      yield new Api.Message({
        id: 500,
        peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
        fromId: new Api.PeerUser({ userId: bigInt(9) }),
        date: fromSeconds + 1,
        message: 'recovery body',
        replyTo: new Api.MessageReplyHeader({ replyToTopId: 77, replyToMsgId: 77 }),
      })
    })
    const service = createRecoveryRepairService({
      context: { emitter: new EventEmitter(), getClient: () => client } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: { takeoutMessages },
      inspect: vi.fn(async () => ({ bindings: [] })),
      historicalGroups: vi.fn(async () => ['-1000000000042']),
      historicalBindingHits: reverseLookup,
      slaveDisplayNames: vi.fn(async () => new Map([['slave.module chat-a', {
        slaveUid: 'slave.module chat-a',
        slaveName: 'Historical Alpha',
        nameSource: 'slavechatinfo.slave_chat_name' as const,
      }]])),
      presences: vi.fn(async () => new Map()),
      insert,
    })

    const updates = await collectUpdates(service({
      etm: { backend: 'sqlite', path: '/unused' },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: (fromSeconds + 10) * 1000,
      chunkSize: 10,
      outputFile: output,
      takeout: true,
    }))

    expect(reverseLookup).toHaveBeenNthCalledWith(1, { backend: 'sqlite', path: '/unused' }, Array.from({ length: 10 }, (_, index) => `-1000000000042.${index + 1}`))
    expect(reverseLookup).toHaveBeenNthCalledWith(2, { backend: 'sqlite', path: '/unused' }, ['-1000000000042.11'])
    expect(insert).toHaveBeenCalledWith(
      { backend: 'sqlite', path: '/unused' },
      [expect.objectContaining({
        identity: '-1000000000042.500',
        binding: expect.objectContaining({ messageThreadId: '77', slaveUid: 'slave.module chat-a' }),
      })],
      10,
    )
    expect(updates).toContainEqual(expect.objectContaining({
      type: 'topic-binding',
      topicChatId: '-1000000000042',
      messageThreadId: '77',
      slaveUid: 'slave.module chat-a',
      source: 'msglog-history',
    }))
    expect(updates).toContainEqual(expect.objectContaining({
      type: 'topic-binding-discovery',
      status: 'completed',
      messageThreadId: '77',
      anchorsExamined: 11,
      outcome: 'resolved',
    }))
    expect(updates).toContainEqual(expect.objectContaining({
      type: 'slave-summary',
      slaveName: 'Historical Alpha',
      counts: expect.objectContaining({ inserted: 1, mappedExamined: 1 }),
    }))
    const report = (await readFile(output, 'utf8')).trim().split('\n').map(line => JSON.parse(line) as Record<string, unknown>)
    expect(report.filter(event => event.type === 'topic-binding-discovery')).toEqual([
      expect.objectContaining({ status: 'started', messageThreadId: '77' }),
      expect.objectContaining({ status: 'completed', anchorsExamined: 11, outcome: 'resolved' }),
    ])
    expect(report).toContainEqual(expect.objectContaining({
      type: 'topic-binding',
      source: 'msglog-history',
      messageThreadId: '77',
      slaveUid: 'slave.module chat-a',
    }))
    expect(await readFile(output, 'utf8')).not.toContain('recovery body')
    expect(takeoutMessages).toHaveBeenCalledTimes(2)
  })

  it('stops historical topic discovery after the first unique anchor batch', async () => {
    const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
    const group = new Api.Channel({
      id: bigInt(42),
      accessHash: bigInt(1),
      title: 'Historical',
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const reverseLookup = vi.fn(async (_source: unknown, identities: string[]) => identities.includes('-1000000000042.1')
      ? new Map([['-1000000000042.1', ['slave.module chat-a']]])
      : new Map())
    let yieldedAnchors = 0
    const service = createRecoveryRepairService({
      context: { emitter: new EventEmitter(), getClient: () => ({ getEntity: vi.fn(async () => group) }) } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: {
        async* takeoutMessages(_chatId, options) {
          if (options.replyTo === 77) {
            for (let id = 1; id <= 20; id++) {
              yieldedAnchors += 1
              yield new Api.Message({ id, peerId: new Api.PeerChannel({ channelId: bigInt(42) }), date: fromSeconds - 100 + id, message: 'anchor' })
            }
            return
          }
          yield new Api.Message({
            id: 500,
            peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
            fromId: new Api.PeerUser({ userId: bigInt(9) }),
            date: fromSeconds + 1,
            message: 'body',
            replyTo: new Api.MessageReplyHeader({ replyToTopId: 77, replyToMsgId: 77 }),
          })
        },
      },
      inspect: vi.fn(async () => ({ bindings: [] })),
      historicalGroups: vi.fn(async () => ['-1000000000042']),
      historicalBindingHits: reverseLookup,
      slaveDisplayNames: vi.fn(async () => new Map()),
      presences: vi.fn(async () => new Map()),
      insert: vi.fn(async (_source: unknown, candidates: RepairCandidate[]) => ({
        inserted: candidates.length,
        concurrent: 0,
        conflicts: 0,
        errors: 0,
        statuses: new Map(candidates.map(candidate => [candidate.identity, 'inserted' as const])),
      })),
    })

    const updates = await collectUpdates(service({
      etm: { backend: 'sqlite', path: '/unused' },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: (fromSeconds + 10) * 1000,
      chunkSize: 10,
      outputFile: null,
      takeout: true,
    }))

    expect(reverseLookup).toHaveBeenCalledOnce()
    expect(yieldedAnchors).toBe(10)
    expect(updates).toContainEqual(expect.objectContaining({
      type: 'topic-binding-discovery',
      status: 'completed',
      anchorsExamined: 10,
      outcome: 'resolved',
      slaveUid: 'slave.module chat-a',
    }))
  })

  it('emits topic discovery heartbeats while awaiting one anchor iterator request', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] })
    try {
      const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
      const group = new Api.Channel({
        id: bigInt(42),
        accessHash: bigInt(1),
        title: 'Historical',
        photo: new Api.ChatPhotoEmpty(),
        date: 0,
        megagroup: true,
      })
      const delayedAnchors = (async function* () {
        await new Promise(resolve => setTimeout(resolve, 90_000))
        yield new Api.Message({ id: 1, peerId: new Api.PeerChannel({ channelId: bigInt(42) }), date: fromSeconds - 1, message: 'anchor' })
      })()
      const anchorNext = vi.spyOn(delayedAnchors, 'next')
      const service = createRecoveryRepairService({
        context: { emitter: new EventEmitter(), getClient: () => ({ getEntity: vi.fn(async () => group) }) } as unknown as CoreContext,
        logger: logger(),
        entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
        takeoutService: {
          takeoutMessages: vi.fn((_chatId, options) => options.replyTo === 77
            ? delayedAnchors
            : (async function* () {
                yield new Api.Message({
                  id: 500,
                  peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
                  fromId: new Api.PeerUser({ userId: bigInt(9) }),
                  date: fromSeconds + 1,
                  message: 'body',
                  replyTo: new Api.MessageReplyHeader({ replyToTopId: 77, replyToMsgId: 77 }),
                })
              })()),
        },
        inspect: vi.fn(async () => ({ bindings: [] })),
        historicalGroups: vi.fn(async () => ['-1000000000042']),
        historicalBindingHits: vi.fn(async () => new Map([['-1000000000042.1', ['slave.module chat-a']]])),
        slaveDisplayNames: vi.fn(async () => new Map()),
        presences: vi.fn(async () => new Map()),
        insert: vi.fn(async (_source: unknown, candidates: RepairCandidate[]) => ({
          inserted: candidates.length,
          concurrent: 0,
          conflicts: 0,
          errors: 0,
          statuses: new Map(candidates.map(candidate => [candidate.identity, 'inserted' as const])),
        })),
      })
      const stream = service({
        etm: { backend: 'sqlite', path: '/unused' },
        mainBotId: '9',
        auxiliaryBotIds: [],
        startedAtMs: (fromSeconds + 10) * 1000,
        chunkSize: 10,
        outputFile: null,
        takeout: true,
      })
      let update = await stream.next()
      while (!update.done && update.value.type !== 'topic-binding-discovery')
        update = await stream.next()
      expect(update.value).toMatchObject({ type: 'topic-binding-discovery', status: 'started', messageThreadId: '77' })

      const pendingHeartbeat = stream.next()
      await vi.advanceTimersByTimeAsync(0)
      await vi.advanceTimersByTimeAsync(60_000)
      await expect(pendingHeartbeat).resolves.toMatchObject({
        value: {
          type: 'topic-binding-discovery-heartbeat',
          messageThreadId: '77',
          anchorsChecked: 0,
          elapsedMs: 60_000,
          idleMs: 60_000,
        },
      })
      expect(anchorNext).toHaveBeenCalledOnce()

      const pendingResult = stream.next()
      await vi.advanceTimersByTimeAsync(0)
      await vi.advanceTimersByTimeAsync(30_000)
      await expect(pendingResult).resolves.toMatchObject({
        value: { type: 'topic-binding-discovery', status: 'completed', outcome: 'resolved', anchorsExamined: 1 },
      })
      expect(anchorNext).toHaveBeenCalledTimes(2)
      let done = false
      while (!done)
        done = (await stream.next()).done === true
    }
    finally {
      vi.useRealTimers()
    }
  })

  it('keeps TopicAssoc authoritative and detects historical ambiguity in one anchor batch', async () => {
    const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
    const group = new Api.Channel({
      id: bigInt(42),
      accessHash: bigInt(1),
      title: 'Mixed',
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const binding = normalizeBindings([
      { topic_chat_id: '-1000000000042', message_thread_id: '10', slave_uid: 'slave.module current' },
    ])[0]
    const reverseLookup = vi.fn(async (_source: unknown, identities: string[]) => {
      if (identities.includes('-1000000000042.1')) {
        return new Map([
          ['-1000000000042.1', ['slave.module old-a']],
          ['-1000000000042.2', ['slave.module old-b']],
        ])
      }
      return new Map()
    })
    const insert = vi.fn(async (_source: unknown, candidates: RepairCandidate[]) => ({
      inserted: candidates.length,
      concurrent: 0,
      conflicts: 0,
      errors: 0,
      statuses: new Map(candidates.map(candidate => [candidate.identity, 'inserted' as const])),
    }))
    const client = { getEntity: vi.fn(async () => group) }
    const service = createRecoveryRepairService({
      context: { emitter: new EventEmitter(), getClient: () => client } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: {
        async* takeoutMessages(_chatId, options) {
          if (options.replyTo === 77) {
            for (let id = 1; id <= 11; id++)
              yield new Api.Message({ id, peerId: new Api.PeerChannel({ channelId: bigInt(42) }), date: fromSeconds - 20 + id, message: `anchor ${id}` })
            return
          }
          for (const [id, topicId] of [[500, 10], [501, 77]] as const) {
            yield new Api.Message({
              id,
              peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
              fromId: new Api.PeerUser({ userId: bigInt(9) }),
              date: fromSeconds + id - 499,
              message: 'body',
              replyTo: new Api.MessageReplyHeader({ replyToTopId: topicId, replyToMsgId: topicId }),
            })
          }
        },
      },
      inspect: vi.fn(async () => ({ bindings: [binding] })),
      historicalGroups: vi.fn(async () => []),
      historicalBindingHits: reverseLookup,
      slaveDisplayNames: vi.fn(async () => new Map()),
      presences: vi.fn(async () => new Map()),
      insert,
    })
    const updates = await collectUpdates(service({
      etm: { backend: 'sqlite', path: '/unused' },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: (fromSeconds + 10) * 1000,
      chunkSize: 10,
      outputFile: null,
      takeout: true,
    }))

    expect(insert).toHaveBeenCalledWith(
      { backend: 'sqlite', path: '/unused' },
      [expect.objectContaining({ identity: '-1000000000042.500', binding })],
      10,
    )
    expect(reverseLookup).toHaveBeenCalledOnce()
    expect(updates).toContainEqual(expect.objectContaining({
      type: 'topic-binding-conflict',
      messageThreadId: '77',
      slaveUids: ['slave.module old-a', 'slave.module old-b'],
    }))
    expect(updates).toContainEqual(expect.objectContaining({
      type: 'topic-binding-discovery',
      status: 'completed',
      anchorsExamined: 10,
      outcome: 'conflict',
      slaveUids: ['slave.module old-a', 'slave.module old-b'],
    }))
    expect(updates.at(-1)).toMatchObject({ summary: { counts: { 'inserted': 1, 'unbound-topic': 1 } } })
  })

  it('skips a stale bound group while importing accessible messages and reporting safe metadata', async () => {
    const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
    const toMs = (fromSeconds + 200) * 1000
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const path = join(directory, 'etm.db')
    const output = join(directory, 'report.jsonl')
    const database = createEtmDatabase(path)
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(1, '-1000000000042', '10', 'slave.module chat-a')
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(2, '-1000000000043', '10', 'slave.module chat-b')
    database.close()
    await writeFile(output, '{"type":"prior-run"}\n', { mode: 0o644 })
    await chmod(output, 0o644)

    const group = new Api.Channel({
      id: bigInt(42),
      accessHash: bigInt(1),
      title: 'Bound',
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const client = { getEntity: vi.fn(async () => group) }
    const getInputPeer = vi.fn(async (chatId: string | number) => {
      if (String(chatId) === '42')
        return new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })
      throw Object.assign(new Error('400: CHANNEL_PRIVATE'), { errorMessage: 'CHANNEL_PRIVATE' })
    })
    const takeoutMessages = vi.fn(async function* () {
      yield new Api.Message({
        id: 150,
        peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
        fromId: new Api.PeerUser({ userId: bigInt(9) }),
        date: fromSeconds + 1,
        message: 'imported private body',
        replyTo: new Api.MessageReplyHeader({ replyToMsgId: 10 }),
      })
    })
    const service = createRecoveryRepairService({
      context: { emitter: new EventEmitter(), getClient: () => client } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer },
      takeoutService: { takeoutMessages },
    })
    const input = {
      etm: { backend: 'sqlite' as const, path },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: toMs,
      chunkSize: 10,
      outputFile: output,
      takeout: true,
    }

    const first = (await collectUpdates(service(input))).at(-1)
    expect(getInputPeer).toHaveBeenCalledWith('42')
    expect(getInputPeer).toHaveBeenCalledWith('43')
    expect(takeoutMessages).toHaveBeenCalledTimes(1)
    expect(first).toMatchObject({
      type: 'completed',
      summary: {
        counts: {
          'inserted': 1,
          'unavailable-bound-group': 1,
          'errors': 0,
        },
        examined: 1,
      },
    })

    const check = new DatabaseSync(path, { readOnly: true })
    try {
      expect(check.prepare('SELECT master_msg_id, text FROM msglog ORDER BY master_msg_id').all()).toEqual([{
        master_msg_id: '-1000000000042.150',
        text: 'imported private body',
      }])
      expect(check.prepare('SELECT COUNT(*) AS count FROM msglog WHERE master_msg_id LIKE ?').get('-1000000000043.%')).toEqual({ count: 0 })
    }
    finally {
      check.close()
    }

    const reportLines = (await readFile(output, 'utf8')).trim().split('\n').map(line => JSON.parse(line) as Record<string, unknown>)
    expect(reportLines).toHaveLength(8)
    expect(reportLines[0]).toEqual({ type: 'prior-run' })
    expect(reportLines[1]).toMatchObject({ type: 'run-start', version: 2, groups: ['-1000000000043', '-1000000000042'] })
    expect(reportLines[2]).toMatchObject({ type: 'group-start', topicChatId: '-1000000000043' })
    expect(reportLines[3]).toMatchObject({
      type: 'group-unavailable',
      version: 2,
      topicChatId: '-1000000000043',
      sourceChatId: '43',
      category: 'channel-private',
      bindingCount: 1,
      bindings: [{
        messageThreadId: '10',
        slaveUid: 'slave.module chat-b',
        slaveName: 'slave.module chat-b',
        nameSource: 'slave_uid',
      }],
      totalCounts: expect.objectContaining({ 'unavailable-bound-group': 1 }),
    })
    expect(reportLines[4]).toMatchObject({ type: 'group-start', topicChatId: '-1000000000042' })
    expect(reportLines[5]).toMatchObject({
      type: 'slave-summary',
      topicChatId: '-1000000000042',
      slaveUid: 'slave.module chat-a',
      counts: expect.objectContaining({ inserted: 1, mappedExamined: 1 }),
    })
    expect(reportLines[6]).toMatchObject({
      type: 'group-complete',
      version: 2,
      topicChatId: '-1000000000042',
      sourceChatId: '42',
      slaveCount: 1,
      mappedExamined: 1,
      counts: expect.objectContaining({ inserted: 1 }),
      candidates: [expect.objectContaining({
        identity: '-1000000000042.150',
        status: 'repair-attempted',
      })],
    })
    expect(reportLines[7]).toMatchObject({
      type: 'run-complete',
      version: 2,
      summary: {
        examined: 1,
        counts: expect.objectContaining({
          'inserted': 1,
          'unavailable-bound-group': 1,
        }),
      },
    })
    expect(await readFile(output, 'utf8')).not.toContain('imported private body')
    expect((await stat(output)).mode & 0o777).toBe(0o600)

    const second = (await collectUpdates(service(input))).at(-1)
    expect(second).toMatchObject({
      summary: {
        counts: {
          'present-primary': 1,
          'inserted': 0,
          'unavailable-bound-group': 1,
        },
      },
    })
  })

  it('does not present a missing local input entity as a verified unavailable group', async () => {
    // A local entity-cache miss does not establish whether the account can access the group.
    const takeoutMessages = vi.fn()
    const service = createRecoveryRepairService({
      context: { emitter: new EventEmitter(), getClient: vi.fn() } as unknown as CoreContext,
      logger: logger(),
      entityService: {
        getInputPeer: vi.fn(async () => {
          throw new Error('Could not find the input entity for {"channelId":"42","className":"PeerChannel"}')
        }),
      },
      takeoutService: { takeoutMessages },
      inspect: vi.fn(async () => ({ bindings: [] })),
      historicalGroups: vi.fn(async () => ['-1000000000042']),
    })

    await expect(collectUpdates(service({
      etm: { backend: 'sqlite', path: '/unused' },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: Date.parse(RECOVERY_REPAIR_FROM_ISO) + 10_000,
      chunkSize: 10,
      outputFile: null,
      takeout: true,
    }))).rejects.toThrow('Could not find the input entity')

    expect(takeoutMessages).not.toHaveBeenCalled()
  })

  it('skips a historical broadcast channel while preserving earlier and later group repairs', async () => {
    const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const path = join(directory, 'etm.db')
    const output = join(directory, 'report.jsonl')
    const database = createEtmDatabase(path)
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(1, '-1000000000044', '10', 'slave.module chat-a')
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(2, '-1000000000042', '10', 'slave.module chat-b')
    insertExisting(database, '-1000000000043.1', null)
    database.close()

    const channel = (id: string, megagroup: boolean) => new Api.Channel({
      id: bigInt(id),
      accessHash: bigInt(1),
      title: `Channel ${id}`,
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup,
    })
    const client = {
      getEntity: vi.fn(async (peer: Api.TypeInputPeer) => {
        if (!(peer instanceof Api.InputPeerChannel))
          throw new Error('unexpected peer')
        const id = peer.channelId.toString()
        return channel(id, id !== '43')
      }),
    }
    const takeoutMessages = vi.fn(async function* (chatId: string) {
      yield new Api.Message({
        id: Number(chatId) + 100,
        peerId: new Api.PeerChannel({ channelId: bigInt(chatId) }),
        fromId: new Api.PeerUser({ userId: bigInt(9) }),
        date: fromSeconds + 1,
        message: `body ${chatId}`,
        replyTo: new Api.MessageReplyHeader({ replyToTopId: 10, replyToMsgId: 10 }),
      })
    })
    const service = createRecoveryRepairService({
      context: { emitter: new EventEmitter(), getClient: () => client } as unknown as CoreContext,
      logger: logger(),
      entityService: {
        getInputPeer: vi.fn(async (chatId: string | number) => new Api.InputPeerChannel({
          channelId: bigInt(String(chatId)),
          accessHash: bigInt(1),
        })),
      },
      takeoutService: { takeoutMessages },
    })

    const updates = await collectUpdates(service({
      etm: { backend: 'sqlite', path },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: (fromSeconds + 10) * 1000,
      chunkSize: 10,
      outputFile: output,
      takeout: true,
    }))

    expect(takeoutMessages.mock.calls.map(call => call[0])).toEqual(['44', '42'])
    expect(updates).toContainEqual(expect.objectContaining({
      type: 'group-unavailable',
      topicChatId: '-1000000000043',
      category: 'broadcast-channel',
      bindingCount: 0,
    }))
    expect(updates.at(-1)).toMatchObject({
      summary: {
        examined: 2,
        counts: { 'inserted': 2, 'unavailable-bound-group': 1, 'errors': 0 },
      },
    })
    const check = new DatabaseSync(path, { readOnly: true })
    try {
      expect(check.prepare(`SELECT master_msg_id FROM msglog WHERE master_msg_id LIKE '-1000000000044.%' OR master_msg_id LIKE '-1000000000042.%' ORDER BY master_msg_id`).all()).toEqual([
        { master_msg_id: '-1000000000042.142' },
        { master_msg_id: '-1000000000044.144' },
      ])
    }
    finally {
      check.close()
    }
    const report = (await readFile(output, 'utf8')).trim().split('\n').map(line => JSON.parse(line) as Record<string, unknown>)
    const completedGroups = report.filter(event => event.type === 'group-complete').map(event => event.topicChatId)
    expect(completedGroups).toEqual(['-1000000000044', '-1000000000042'])
    expect(report).toContainEqual(expect.objectContaining({
      type: 'group-unavailable',
      topicChatId: '-1000000000043',
      category: 'broadcast-channel',
    }))
    expect(await readFile(output, 'utf8')).not.toContain('body 44')
    expect(await readFile(output, 'utf8')).not.toContain('body 42')
  })

  it('keeps completed group rows and report events after a later failure, then reruns idempotently', async () => {
    const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
    const toMs = (fromSeconds + 200) * 1000
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const path = join(directory, 'etm.db')
    const output = join(directory, 'report.jsonl')
    const database = createEtmDatabase(path)
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(1, '-1000000000043', '10', 'slave.module chat-a')
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(2, '-1000000000042', '10', 'slave.module chat-b')
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(3, '-1000000000041', '10', 'slave.module chat-c')
    database.close()

    let secondRun = false
    const channel = (id: string) => new Api.Channel({
      id: bigInt(id),
      accessHash: bigInt(1),
      title: `Bound ${id}`,
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const client = {
      getEntity: vi.fn(async (peer: Api.TypeInputPeer) => {
        if (peer instanceof Api.InputPeerChannel)
          return channel(peer.channelId.toString())
        throw new Error('unexpected peer')
      }),
    }
    const getInputPeer = vi.fn(async (chatId: string | number) => {
      if (String(chatId) === '42' && secondRun)
        throw Object.assign(new Error('400: CHANNEL_PRIVATE'), { errorMessage: 'CHANNEL_PRIVATE' })
      return new Api.InputPeerChannel({ channelId: bigInt(String(chatId)), accessHash: bigInt(1) })
    })
    const takeoutMessages = vi.fn(async function* (chatId: string) {
      if (chatId === '42') {
        yield* []
        throw Object.assign(new Error('500: INTERNAL (caused by messages.GetHistory)'), {
          code: 500,
          errorMessage: 'INTERNAL',
        })
      }
      yield new Api.Message({
        id: chatId === '43' ? 250 : 350,
        peerId: new Api.PeerChannel({ channelId: bigInt(chatId) }),
        fromId: new Api.PeerUser({ userId: bigInt(9) }),
        date: fromSeconds + 1,
        message: chatId === '43' ? 'alpha private body' : 'charlie private body',
        replyTo: new Api.MessageReplyHeader({ replyToMsgId: 10 }),
      })
    })
    const service = createRecoveryRepairService({
      context: { emitter: new EventEmitter(), getClient: () => client } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer },
      takeoutService: { takeoutMessages },
    })
    const input = {
      etm: { backend: 'sqlite' as const, path },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: toMs,
      chunkSize: 10,
      outputFile: output,
      takeout: true,
    }

    await expect(collectUpdates(service(input))).rejects.toThrow('INTERNAL')
    let check = new DatabaseSync(path, { readOnly: true })
    try {
      expect(check.prepare('SELECT master_msg_id, text FROM msglog ORDER BY master_msg_id').all()).toEqual([{
        master_msg_id: '-1000000000043.250',
        text: 'alpha private body',
      }])
    }
    finally {
      check.close()
    }
    let reportLines = (await readFile(output, 'utf8')).trim().split('\n').map(line => JSON.parse(line) as Record<string, unknown>)
    expect(reportLines.map(line => line.type)).toEqual(['run-start', 'group-start', 'slave-summary', 'group-complete', 'group-start', 'run-failed'])
    expect(reportLines[2]).toMatchObject({
      type: 'slave-summary',
      topicChatId: '-1000000000043',
      slaveUid: 'slave.module chat-a',
      counts: expect.objectContaining({ inserted: 1, mappedExamined: 1 }),
    })
    expect(reportLines[3]).toMatchObject({
      topicChatId: '-1000000000043',
      counts: expect.objectContaining({ inserted: 1 }),
    })
    expect(reportLines[5]).toMatchObject({
      type: 'run-failed',
      category: 'telegram-internal',
      totalCounts: expect.objectContaining({ inserted: 1 }),
      totalExamined: 1,
    })

    secondRun = true
    const second = (await collectUpdates(service(input))).at(-1)
    expect(second).toMatchObject({
      summary: {
        counts: {
          'present-primary': 1,
          'inserted': 1,
          'unavailable-bound-group': 1,
        },
        examined: 2,
      },
    })
    check = new DatabaseSync(path, { readOnly: true })
    try {
      expect(check.prepare('SELECT master_msg_id, text FROM msglog ORDER BY master_msg_id').all()).toEqual([
        {
          master_msg_id: '-1000000000041.350',
          text: 'charlie private body',
        },
        {
          master_msg_id: '-1000000000043.250',
          text: 'alpha private body',
        },
      ])
      expect(check.prepare('SELECT COUNT(*) AS count FROM msglog WHERE master_msg_id = ?').get('-1000000000043.250')).toEqual({ count: 1 })
    }
    finally {
      check.close()
    }

    const report = await readFile(output, 'utf8')
    expect(report).not.toContain('alpha private body')
    expect(report).not.toContain('charlie private body')
    reportLines = report.trim().split('\n').map(line => JSON.parse(line) as Record<string, unknown>)
    expect(reportLines.map(line => line.type)).toEqual([
      'run-start',
      'group-start',
      'slave-summary',
      'group-complete',
      'group-start',
      'run-failed',
      'run-start',
      'group-start',
      'slave-summary',
      'group-complete',
      'group-start',
      'group-unavailable',
      'group-start',
      'slave-summary',
      'group-complete',
      'run-complete',
    ])
    expect(reportLines[11]).toMatchObject({
      type: 'group-unavailable',
      topicChatId: '-1000000000042',
      sourceChatId: '42',
      category: 'channel-private',
    })
  })

  it('reports channel-invalid bound groups without writing rows', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const path = join(directory, 'etm.db')
    const output = join(directory, 'report.jsonl')
    const database = createEtmDatabase(path)
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(1, '-1000000000042', '10', 'slave.module chat-a')
    database.close()
    const service = createRecoveryRepairService({
      context: {
        emitter: new EventEmitter(),
        getClient: () => ({
          getEntity: vi.fn(async () => {
            throw Object.assign(new Error('400: CHANNEL_INVALID (caused by channels.GetChannels)'), {
              code: 400,
              errorMessage: 'CHANNEL_INVALID',
            })
          }),
        }),
      } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: { takeoutMessages: vi.fn() },
    })

    const last = (await collectUpdates(service({
      etm: { backend: 'sqlite', path },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: Date.parse(RECOVERY_REPAIR_FROM_ISO) + 10_000,
      chunkSize: 10,
      outputFile: output,
      takeout: true,
    }))).at(-1)
    expect(last).toMatchObject({
      summary: {
        counts: {
          'inserted': 0,
          'unavailable-bound-group': 1,
        },
      },
    })

    const check = new DatabaseSync(path, { readOnly: true })
    try {
      expect(check.prepare('SELECT COUNT(*) AS count FROM msglog').get()).toEqual({ count: 0 })
    }
    finally {
      check.close()
    }

    const report = (await readFile(output, 'utf8')).trim().split('\n').map(line => JSON.parse(line) as Record<string, unknown>)
    expect(report.map(line => line.type)).toEqual(['run-start', 'group-start', 'group-unavailable', 'run-complete'])
    expect(report[2]).toMatchObject({
      type: 'group-unavailable',
      topicChatId: '-1000000000042',
      sourceChatId: '42',
      category: 'channel-invalid',
      bindingCount: 1,
      bindings: [{
        messageThreadId: '10',
        slaveUid: 'slave.module chat-a',
        slaveName: 'slave.module chat-a',
        nameSource: 'slave_uid',
      }],
    })
  })

  it('aborts transient Telegram failures without reporting an unavailable bound group', async () => {
    const group = new Api.Channel({
      id: bigInt(42),
      accessHash: bigInt(1),
      title: 'Bound',
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const insert = vi.fn()
    const service = createRecoveryRepairService({
      context: {
        emitter: new EventEmitter(),
        getClient: () => ({ getEntity: vi.fn(async () => group) }),
      } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: {
        async* takeoutMessages() {
          yield* []
          throw Object.assign(new Error('500: INTERNAL (caused by messages.GetHistory)'), {
            code: 500,
            errorMessage: 'INTERNAL',
          })
        },
      },
      inspect: vi.fn(async () => ({
        bindings: [{
          topicChatId: '-1000000000042',
          messageThreadId: '10',
          slaveUid: 'slave.module chat-a',
          slaveModule: 'slave.module',
        }],
      })),
      insert,
    })

    await expect(collectUpdates(service({
      etm: { backend: 'sqlite', path: '/unused' },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: Date.parse(RECOVERY_REPAIR_FROM_ISO) + 10_000,
      chunkSize: 10,
      outputFile: null,
      takeout: true,
    }))).rejects.toThrow('INTERNAL')
    expect(insert).not.toHaveBeenCalled()
  })

  it('imports messages by exact topic key when one slave UID has multiple topic rows', async () => {
    const fromSeconds = Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000
    const group = new Api.Channel({
      id: bigInt(42),
      accessHash: bigInt(1),
      title: 'Bound',
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const getInputPeer = vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) }))
    const insert = vi.fn(async () => ({ inserted: 2, concurrent: 0, conflicts: 0, errors: 0 }))
    const bindings = normalizeBindings([
      { topic_chat_id: '-1000000000042', message_thread_id: '10', slave_uid: 'slave.module chat-a' },
      { topic_chat_id: '-1000000000042', message_thread_id: '11', slave_uid: 'slave.module chat-a' },
    ])
    const service = createRecoveryRepairService({
      context: {
        emitter: new EventEmitter(),
        getClient: () => ({ getEntity: vi.fn(async () => group) }),
      } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer },
      takeoutService: {
        async* takeoutMessages() {
          yield new Api.Message({
            id: 150,
            peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
            fromId: new Api.PeerUser({ userId: bigInt(9) }),
            date: fromSeconds + 1,
            message: 'first topic text',
            replyTo: new Api.MessageReplyHeader({ replyToTopId: 10, replyToMsgId: 10 }),
          })
          yield new Api.Message({
            id: 151,
            peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
            fromId: new Api.PeerUser({ userId: bigInt(9) }),
            date: fromSeconds + 2,
            message: 'second topic text',
            replyTo: new Api.MessageReplyHeader({ replyToTopId: 11, replyToMsgId: 11 }),
          })
        },
      },
      inspect: vi.fn(async () => ({ bindings })),
      presences: vi.fn(async () => new Map()),
      insert,
    })

    const updates = []
    for await (const update of service({
      etm: { backend: 'sqlite', path: '/unused' },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: Date.parse(RECOVERY_REPAIR_FROM_ISO) + 10_000,
      chunkSize: 10,
      outputFile: null,
      takeout: true,
    })) updates.push(update)

    expect(getInputPeer).toHaveBeenCalledTimes(1)
    expect(insert).toHaveBeenCalledWith(
      { backend: 'sqlite', path: '/unused' },
      [
        expect.objectContaining({
          identity: '-1000000000042.150',
          text: 'first topic text',
          binding: expect.objectContaining({ messageThreadId: '10', slaveUid: 'slave.module chat-a' }),
        }),
        expect.objectContaining({
          identity: '-1000000000042.151',
          text: 'second topic text',
          binding: expect.objectContaining({ messageThreadId: '11', slaveUid: 'slave.module chat-a' }),
        }),
      ],
      10,
    )
    expect(updates.at(-1)).toMatchObject({ summary: { counts: { inserted: 2 } } })
  })

  it('aborts before Telegram acquisition when a topic key maps to different slave UIDs', async () => {
    const getInputPeer = vi.fn()
    const insert = vi.fn()
    const service = createRecoveryRepairService({
      context: { emitter: new EventEmitter(), getClient: vi.fn() } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer },
      takeoutService: { takeoutMessages: vi.fn() },
      inspect: vi.fn(async () => ({
        bindings: normalizeBindings([
          { topic_chat_id: '-1000000000042', message_thread_id: '10', slave_uid: 'slave.module chat-a' },
          { topic_chat_id: '-1000000000042', message_thread_id: '10', slave_uid: 'slave.module chat-b' },
        ]),
      })),
      insert,
    })
    const run = async () => {
      for await (const update of service({
        etm: { backend: 'sqlite', path: '/unused' },
        mainBotId: '9',
        auxiliaryBotIds: [],
        startedAtMs: Date.parse(RECOVERY_REPAIR_FROM_ISO) + 10_000,
        chunkSize: 10,
        outputFile: null,
        takeout: true,
      })) void update
      return 'completed'
    }

    await expect(run()).rejects.toThrow('Conflicting ETM TopicAssoc mapping for -1000000000042.10')
    expect(getInputPeer).not.toHaveBeenCalled()
    expect(insert).not.toHaveBeenCalled()
  })

  it('aborts before insertion when TopicAssoc changes during acquisition', async () => {
    const bindings = [{
      topicChatId: '-1000000000042',
      messageThreadId: '10',
      slaveUid: 'slave.module chat-a',
      slaveModule: 'slave.module',
    }]
    const inspect = vi.fn()
      .mockResolvedValueOnce({ bindings })
      .mockResolvedValueOnce({ bindings: [{ ...bindings[0], messageThreadId: '11' }] })
    const insert = vi.fn()
    const bot = new Api.User({ id: bigInt(9), firstName: 'Main', username: 'main_bot', bot: true })
    const group = new Api.Channel({ id: bigInt(42), accessHash: bigInt(1), title: 'Bound', photo: new Api.ChatPhotoEmpty(), date: 0, megagroup: true })
    const context = {
      emitter: new EventEmitter(),
      getClient: () => ({ getEntity: vi.fn(async peer => peer instanceof Api.PeerUser ? bot : group) }),
    } as unknown as CoreContext
    const service = createRecoveryRepairService({
      context,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: { async* takeoutMessages() { yield* [] } },
      inspect,
      insert,
    })
    const run = async () => {
      for await (const update of service({
        etm: { backend: 'sqlite', path: '/unused' },
        mainBotId: '9',
        auxiliaryBotIds: [],
        startedAtMs: Date.now(),
        chunkSize: 10,
        outputFile: null,
        takeout: true,
      })) void update
      return 'completed'
    }
    await expect(run()).rejects.toThrow('during Telegram acquisition')
    expect(insert).not.toHaveBeenCalled()
  })

  it('trusts configured bot-token IDs without owner-session user resolution and filters senders exactly', async () => {
    const mainBotId = '8465204282'
    const binding = {
      topicChatId: '-1000000000042',
      messageThreadId: '10',
      slaveUid: 'slave.module chat-a',
      slaveModule: 'slave.module',
    }
    const group = new Api.Channel({
      id: bigInt(42),
      accessHash: bigInt(1),
      title: 'Bound',
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const getEntity = vi.fn(async (peer: unknown) => {
      if (peer instanceof Api.PeerUser)
        throw new Error('owner session has no cached bot entity')
      return group
    })
    const insert = vi.fn(async () => ({ inserted: 1, concurrent: 0, conflicts: 0, errors: 0 }))
    const service = createRecoveryRepairService({
      context: {
        emitter: new EventEmitter(),
        getClient: () => ({ getEntity }),
      } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: {
        async* takeoutMessages() {
          yield new Api.Message({
            id: 150,
            peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
            fromId: new Api.PeerUser({ userId: bigInt(mainBotId) }),
            date: Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000 + 1,
            message: 'configured bot text',
            replyTo: new Api.MessageReplyHeader({ replyToMsgId: 10 }),
          })
          yield new Api.Message({
            id: 151,
            peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
            fromId: new Api.PeerUser({ userId: bigInt('846520428') }),
            date: Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000 + 2,
            message: 'prefix-like text',
            replyTo: new Api.MessageReplyHeader({ replyToMsgId: 10 }),
          })
          yield new Api.Message({
            id: 152,
            peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
            fromId: new Api.PeerUser({ userId: bigInt('84652042820') }),
            date: Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000 + 3,
            message: 'suffix-like text',
            replyTo: new Api.MessageReplyHeader({ replyToMsgId: 10 }),
          })
        },
      },
      inspect: vi.fn(async () => ({ bindings: [binding] })),
      presences: vi.fn(async () => new Map()),
      insert,
    })

    const updates = []
    for await (const update of service({
      etm: { backend: 'sqlite', path: '/unused' },
      mainBotId,
      auxiliaryBotIds: [],
      startedAtMs: Date.parse(RECOVERY_REPAIR_FROM_ISO) + 10_000,
      chunkSize: 10,
      outputFile: null,
      takeout: true,
    })) updates.push(update)

    expect(getEntity).not.toHaveBeenCalledWith(expect.any(Api.PeerUser))
    expect(insert).toHaveBeenCalledWith(
      { backend: 'sqlite', path: '/unused' },
      [expect.objectContaining({
        identity: '-1000000000042.150',
        senderId: mainBotId,
        text: 'configured bot text',
      })],
      10,
    )
    expect(updates.at(-1)).toMatchObject({
      summary: {
        counts: {
          'human-or-unconfigured-sender': 2,
          'inserted': 1,
        },
      },
    })
  })

  it('counts records that become present after the initial snapshot as concurrent conflicts', async () => {
    const bot = new Api.User({ id: bigInt(9), firstName: 'Main', username: 'main_bot', bot: true })
    const group = new Api.Channel({ id: bigInt(42), accessHash: bigInt(1), title: 'Bound', photo: new Api.ChatPhotoEmpty(), date: 0, megagroup: true })
    const context = {
      emitter: new EventEmitter(),
      getClient: () => ({ getEntity: vi.fn(async peer => peer instanceof Api.PeerUser && peer.userId.toString() === '9' ? bot : group) }),
    } as unknown as CoreContext
    const binding = { topicChatId: '-1000000000042', messageThreadId: '10', slaveUid: 'slave.module chat-a', slaveModule: 'slave.module' }
    const service = createRecoveryRepairService({
      context,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: {
        async* takeoutMessages() {
          yield new Api.Message({
            id: 150,
            peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
            fromId: new Api.PeerUser({ userId: bigInt(9) }),
            date: Date.parse(RECOVERY_REPAIR_FROM_ISO) / 1000 + 1,
            message: 'text',
            replyTo: new Api.MessageReplyHeader({ replyToMsgId: 10 }),
          })
        },
      },
      inspect: vi.fn(async () => ({
        bindings: [binding],
      })),
      presences: vi.fn(async () => new Map()),
      insert: vi.fn(async () => ({ inserted: 0, concurrent: 1, conflicts: 0, errors: 0 })),
    })
    const updates = []
    for await (const update of service({
      etm: { backend: 'sqlite', path: '/unused' },
      mainBotId: '9',
      auxiliaryBotIds: [],
      startedAtMs: Date.now(),
      chunkSize: 10,
      outputFile: null,
      takeout: true,
    })) updates.push(update)
    expect(updates.at(-1)).toMatchObject({ summary: { counts: { concurrent: 1, inserted: 0 } } })
  })

  it('uses a short PostgreSQL table-lock transaction and rechecks both identity columns before insertion', async () => {
    const credential = 'secret'
    const statements: string[] = []
    const query = vi.fn(async (query: unknown) => {
      const sql = String(query)
      statements.push(sql)
      if (sql.startsWith('SELECT master_msg_id'))
        return { rows: [], rowCount: 0 }
      if (sql.startsWith('INSERT INTO msglog'))
        return { rows: [], rowCount: 1 }
      return { rows: [], rowCount: null }
    })
    vi.spyOn(Pool.prototype, 'connect').mockResolvedValue({ query, release: vi.fn() } as never)
    vi.spyOn(Pool.prototype, 'end').mockResolvedValue()
    const binding = { topicChatId: '-1000000000042', messageThreadId: '10', slaveUid: 'slave.module chat-a', slaveModule: 'slave.module' }
    const source = {
      backend: 'postgres' as const,
      database: 'custom',
      host: 'db.internal',
      port: 5544,
      user: 'etm',
      password: credential,
      maxConnections: 3,
      staleTimeout: 999,
      options: '-c timezone=UTC',
    }

    expect(postgresPoolConfig(source)).toEqual({
      database: 'custom',
      host: 'db.internal',
      port: 5544,
      user: 'etm',
      password: credential,
      max: 3,
      options: '-c timezone=UTC',
    })
    expect(postgresPoolConfig(source)).not.toHaveProperty('idleTimeoutMillis')

    const outcome = await insertRepairCandidates(source, [{
      identity: '-1000000000042.150',
      topicChatId: '-1000000000042',
      messageId: '150',
      senderId: '9',
      senderBotId: null,
      timestamp: 150,
      text: 'text',
      binding,
    }], 1)
    expect(outcome).toMatchObject({ inserted: 1, concurrent: 0, conflicts: 0, errors: 0 })
    expect(outcome.statuses).toEqual(new Map([['-1000000000042.150', 'inserted']]))

    expect(statements).toEqual([
      'BEGIN',
      'LOCK TABLE msglog IN SHARE ROW EXCLUSIVE MODE',
      expect.stringContaining('master_msg_id_alt = ANY'),
      expect.stringContaining('INSERT INTO msglog'),
      'COMMIT',
    ])
  })

  it('propagates database write failures after rolling back the active chunk', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'tg-repair-'))
    temporaryDirectories.push(directory)
    const sqlitePath = join(directory, 'empty.db')
    const binding = {
      topicChatId: '-1000000000042',
      messageThreadId: '10',
      slaveUid: 'slave.module chat-a',
      slaveModule: 'slave.module',
    }
    const candidate = {
      identity: '-1000000000042.150',
      topicChatId: '-1000000000042',
      messageId: '150',
      senderId: '9',
      senderBotId: null,
      timestamp: 150,
      text: 'text',
      binding,
    }
    await expect(
      insertRepairCandidates({ backend: 'sqlite', path: sqlitePath }, [candidate], 1),
    ).rejects.toThrow('no such table: msglog')

    const statements: string[] = []
    const query = vi.fn(async (query: unknown) => {
      const sql = String(query)
      statements.push(sql)
      if (sql.startsWith('INSERT INTO msglog'))
        throw new Error('postgres write failed')
      return { rows: [], rowCount: null }
    })
    vi.spyOn(Pool.prototype, 'connect').mockResolvedValue({ query, release: vi.fn() } as never)
    vi.spyOn(Pool.prototype, 'end').mockResolvedValue()
    const password = ['test', 'credential'].join('-')

    await expect(insertRepairCandidates({
      backend: 'postgres',
      database: 'custom',
      host: 'db.internal',
      port: 5544,
      user: 'etm',
      password,
      maxConnections: 3,
      staleTimeout: 999,
      options: '-c timezone=UTC',
    }, [candidate], 1)).rejects.toThrow('postgres write failed')
    expect(statements).toContain('ROLLBACK')
  })
})
