import type { Logger } from '@guiiai/logg'

import type { CoreContext } from '../../context'
import type { TakeoutService } from '../takeout'

import process from 'node:process'

import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

import bigInt from 'big-integer'

import { EventEmitter } from 'eventemitter3'
import { Api } from 'telegram'
import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  classifyAuditMessages,
  createRecoveryAuditService,
  normalizeBindings,
  parseEtmGroupId,
  readEtmSnapshot,
} from '../recovery-audit'

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
    CREATE TABLE msglog (master_msg_id TEXT PRIMARY KEY, master_msg_id_alt TEXT, sender_bot_id TEXT);
  `)
  return database
}

afterEach(async () => {
  vi.restoreAllMocks()
  await Promise.all(temporaryDirectories.splice(0).map(path => rm(path, { recursive: true, force: true })))
})

describe('eTM read-only discovery', () => {
  it('normalizes -100 groups without numeric coercion and rejects conflicting mappings', () => {
    expect(parseEtmGroupId('-10012345678901234567890')).toEqual({
      topicChatId: '-10012345678901234567890',
      sourceChatId: '10012345677901234567890',
      expectedPeer: 'channel',
    })
    expect(() => normalizeBindings([
      { topic_chat_id: '-42', message_thread_id: '7', slave_uid: 'chat-a' },
      { topic_chat_id: '-42', message_thread_id: '7', slave_uid: 'chat-b' },
    ])).toThrow('Conflicting')
  })

  it('snapshots a real SQLite file read-only with both MsgLog identity columns', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'tg-audit-'))
    temporaryDirectories.push(directory)
    const path = join(directory, 'etm.db')
    const database = createEtmDatabase(path)
    database.prepare('INSERT INTO topicassoc VALUES (?, ?, ?, ?)').run(1, '-1000000000042', '10', 'slave-a')
    database.prepare('INSERT INTO msglog VALUES (?, ?, ?)').run('-1000000000042.11', '-1000000000042.12', '99')
    database.close()

    await expect(readEtmSnapshot({ backend: 'sqlite', path }, true)).resolves.toEqual({
      bindings: [{ topicChatId: '-1000000000042', messageThreadId: '10', slaveUid: 'slave-a' }],
      msgLogs: [{ primary: '-1000000000042.11', alternate: '-1000000000042.12', senderBotId: '99' }],
    })
  })

  it.runIf(Boolean(process.env.RECOVERY_AUDIT_POSTGRES_URL))('snapshots a real PostgreSQL ETM schema through the read-only adapter', async () => {
    const snapshot = await readEtmSnapshot({
      backend: 'postgres',
      url: process.env.RECOVERY_AUDIT_POSTGRES_URL!,
    }, true)
    expect(snapshot.bindings.length).toBeGreaterThan(0)
  })
})

describe('audit classification', () => {
  it('distinguishes both MsgLog columns, missing, unbound, and non-bot evidence', () => {
    const rows = classifyAuditMessages([
      { topicChatId: '-42', sourceChatId: '42', messageId: '1', senderId: '9', timestamp: 1, topicId: '10' },
      { topicChatId: '-42', sourceChatId: '42', messageId: '2', senderId: '9', timestamp: 2, topicId: '10' },
      { topicChatId: '-42', sourceChatId: '42', messageId: '3', senderId: '9', timestamp: 3, topicId: '10' },
      { topicChatId: '-42', sourceChatId: '42', messageId: '4', senderId: '9', timestamp: 4, topicId: '99' },
      { topicChatId: '-42', sourceChatId: '42', messageId: '5', senderId: '8', timestamp: 5, topicId: '10' },
    ], {
      bindings: [{ topicChatId: '-42', messageThreadId: '10', slaveUid: 'slave-a' }],
      msgLogs: [
        { primary: '-42.1', alternate: null, senderBotId: null },
        { primary: '-42.20', alternate: '-42.2', senderBotId: '9' },
      ],
    }, new Set(['9']))
    expect(rows.map(row => row.classification)).toEqual([
      'present-primary',
      'present-alt',
      'missing-at-snapshot',
      'unbound-topic',
      'human-or-unverified-sender',
    ])
    expect(rows.every(row => row.nonImportable)).toBe(true)
  })
})

describe('bounded recovery audit', () => {
  it('acquires each DB-derived group once, enforces [from,to), and produces deterministic reruns', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'tg-audit-'))
    temporaryDirectories.push(directory)
    const output = join(directory, 'audit.jsonl')
    const selected: string[] = []
    const bot = new Api.User({ id: bigInt(9), firstName: 'ETM', bot: true })
    const group = new Api.Channel({
      id: bigInt(42),
      accessHash: bigInt(1),
      title: 'Bound',
      photo: new Api.ChatPhotoEmpty(),
      date: 0,
      megagroup: true,
    })
    const client = {
      getEntity: vi.fn(async (peer: unknown) => typeof peer === 'string' ? bot : group),
    }
    const context = { emitter: new EventEmitter(), getClient: () => client } as unknown as CoreContext
    const messages = [99, 100, 150, 200].map(date => new Api.Message({
      id: date,
      peerId: new Api.PeerChannel({ channelId: bigInt(42) }),
      fromId: new Api.PeerUser({ userId: bigInt(9) }),
      date,
      message: 'private content must not appear',
      replyTo: new Api.MessageReplyHeader({ replyToMsgId: 10, replyToTopId: 10 }),
    }))
    const takeoutMessages = vi.fn(async function* (_chatId: string, options: Parameters<TakeoutService['takeoutMessages']>[1]) {
      selected.push(_chatId)
      expect(options.startTime).toBe(100_000)
      expect(options.endTime).toBe(199_999)
      yield* messages
    })
    const snapshots = vi.fn(async (_source, includeLogs: boolean) => ({
      bindings: [{ topicChatId: '-1000000000042', messageThreadId: '10', slaveUid: 'slave-a' }],
      msgLogs: includeLogs ? [{ primary: '-1000000000042.100', alternate: null, senderBotId: '9' }] : [],
    }))
    const service = createRecoveryAuditService({
      context,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: { takeoutMessages },
      readSnapshot: snapshots,
    })
    const input = { etm: { backend: 'sqlite' as const, path: '/unused' }, fromMs: 100_000, toMs: 200_000, outputFile: output, takeout: true }
    for await (const _update of service(input)) void _update
    const first = await readFile(output, 'utf8')
    for await (const _update of service(input)) void _update
    const second = await readFile(output, 'utf8')

    expect(selected).toEqual(['42', '42'])
    expect(first).toBe(second)
    expect(first).not.toContain('private content')
    const records = first.trim().split('\n').map(line => JSON.parse(line))
    expect(records[0].counts).toMatchObject({ 'present-primary': 1, 'missing-at-snapshot': 1 })
    expect(records.slice(1).map(row => row.messageId)).toEqual(['100', '150'])
  })

  it('aborts when TopicAssoc changes during network acquisition', async () => {
    const bindings = [{ topicChatId: '-1000000000042', messageThreadId: '10', slaveUid: 'slave-a' }]
    const snapshots = vi.fn()
      .mockResolvedValueOnce({ bindings, msgLogs: [] })
      .mockResolvedValueOnce({ bindings: [{ ...bindings[0], messageThreadId: '11' }], msgLogs: [] })
    const context = {
      emitter: new EventEmitter(),
      getClient: () => ({ getEntity: vi.fn(async () => new Api.Channel({ id: bigInt(42), accessHash: bigInt(1), title: 'Bound', photo: new Api.ChatPhotoEmpty(), date: 0, megagroup: true })) }),
    } as unknown as CoreContext
    const service = createRecoveryAuditService({
      context,
      logger: logger(),
      entityService: { getInputPeer: vi.fn(async () => new Api.InputPeerChannel({ channelId: bigInt(42), accessHash: bigInt(1) })) },
      takeoutService: { async* takeoutMessages() { yield* [] } },
      readSnapshot: snapshots,
    })
    const run = async () => {
      for await (const _update of service({
        etm: { backend: 'sqlite', path: '/unused' },
        fromMs: 100,
        toMs: 200,
        outputFile: '/unused',
        takeout: true,
      })) void _update
    }
    await expect(run()).rejects.toThrow('changed during Telegram acquisition')
  })
})
