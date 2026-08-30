import type { Logger } from '@guiiai/logg'

import type { CoreContext } from '../../context'
import type { TakeoutService } from '../takeout'

import process from 'node:process'

import { mkdtemp, readFile, rm } from 'node:fs/promises'
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

  it('inserts mapped main and auxiliary bot text, filters other records, and reruns idempotently', async () => {
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
    const message = (id: number, sender: number, text: string, top?: number, reply?: number) => new Api.Message({
      id,
      peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
      fromId: new Api.PeerUser({ userId: bigInt(sender) }),
      date: id === 99 ? fromSeconds - 1 : fromSeconds + id,
      message: text,
      replyTo: top || reply ? new Api.MessageReplyHeader({ replyToMsgId: reply ?? top!, replyToTopId: top }) : undefined,
    })
    const messages = [
      message(99, 9, 'outside', 10),
      message(100, 9, 'present primary', 10),
      message(101, 9, 'present alternate', 10),
      message(102, 9, 'main text', 10),
      message(103, 10, 'aux text', undefined, 10),
      message(104, 11, 'human text', 10),
      message(107, 12, 'unknown bot text', 10),
      message(105, 9, 'wrong topic', 99),
      message(106, 9, '', 10),
      message(200, 9, 'outside', 10),
    ]
    const takeoutMessages = vi.fn(async function* (_chatId: string, options: Parameters<TakeoutService['takeoutMessages']>[1]) {
      expect(options.startTime).toBe(Date.parse(RECOVERY_REPAIR_FROM_ISO))
      expect(options.endTime).toBe(toMs - 1)
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

    const run = async () => {
      const updates = []
      for await (const update of service(input)) updates.push(update)
      return updates.at(-1)
    }
    const first = await run()
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
      slave_member_uid, media_type, mime, msg_type, sent_to, sender_bot_id
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
        msg_type: 'Text',
        sent_to: 'blueset.telegram',
        sender_bot_id: '10',
      },
    ])

    const second = await run()
    expect(second).toMatchObject({ summary: { counts: { 'present-primary': 3, 'present-alt': 1, 'inserted': 0 } } })
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
    await expect(run()).rejects.toThrow('changed during Telegram acquisition')
    expect(insert).not.toHaveBeenCalled()
  })

  it('rejects a configured identity that is not a matching bot before database inspection', async () => {
    const inspect = vi.fn()
    const service = createRecoveryRepairService({
      context: {
        emitter: new EventEmitter(),
        getClient: () => ({ getEntity: vi.fn(async () => new Api.User({ id: bigInt(9), firstName: 'Human', bot: false })) }),
      } as unknown as CoreContext,
      logger: logger(),
      entityService: { getInputPeer: vi.fn() },
      takeoutService: { takeoutMessages: vi.fn() },
      inspect,
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
    }
    await expect(run()).rejects.toThrow('Configured ETM bot ID 9')
    expect(inspect).not.toHaveBeenCalled()
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

    await expect(insertRepairCandidates(source, [{
      identity: '-1000000000042.150',
      topicChatId: '-1000000000042',
      messageId: '150',
      senderId: '9',
      senderBotId: null,
      timestamp: 150,
      text: 'text',
      binding,
    }], 1)).resolves.toEqual({ inserted: 1, concurrent: 0, conflicts: 0, errors: 0 })

    expect(statements).toEqual([
      'BEGIN',
      'LOCK TABLE msglog IN SHARE ROW EXCLUSIVE MODE',
      expect.stringContaining('master_msg_id_alt = ANY'),
      expect.stringContaining('INSERT INTO msglog'),
      'COMMIT',
    ])
  })
})
