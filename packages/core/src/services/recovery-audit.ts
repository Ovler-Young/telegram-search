import type { Logger } from '@guiiai/logg'
import type { RecoveryAuditInput, RecoveryAuditUpdate } from '@tg-search/protocol'

import type { CoreContext } from '../context'
import type { EntityService } from './entity'
import type { TakeoutService } from './takeout'

import { mkdir, rename, rm, writeFile } from 'node:fs/promises'
import { basename, dirname } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

import { Client } from 'pg'
import { Api } from 'telegram'
import { v4 as uuidv4 } from 'uuid'

import { createTask } from '../utils/task'

const BOT_API_CHANNEL_MARK = 1_000_000_000_000n
const TOPIC_COLUMNS = ['topic_chat_id', 'message_thread_id', 'slave_uid'] as const
const MSGLOG_COLUMNS = ['master_msg_id', 'master_msg_id_alt', 'sender_bot_id'] as const

export interface TopicBinding {
  topicChatId: string
  messageThreadId: string
  slaveUid: string
}

export interface MsgLogIdentity {
  primary: string
  alternate: string | null
  senderBotId: string | null
}

export interface EtmSnapshot {
  bindings: TopicBinding[]
  msgLogs: MsgLogIdentity[]
}

export type DiagnosticClassification
  = 'present-primary'
    | 'present-alt'
    | 'missing-at-snapshot'
    | 'unbound-topic'
    | 'human-or-unverified-sender'

interface AuditMessage {
  topicChatId: string
  sourceChatId: string
  messageId: string
  senderId: string
  timestamp: number
  topicId: string
}

function canonicalInteger(value: unknown, label: string): string {
  const text = String(value)
  if (!/^-?\d+$/.test(text))
    throw new Error(`Invalid ${label}: ${text}`)
  const canonical = BigInt(text).toString()
  if (canonical === '0')
    throw new Error(`${label} must not be zero`)
  return canonical
}

export function parseEtmGroupId(value: string): { topicChatId: string, sourceChatId: string, expectedPeer: 'channel' | 'chat' } {
  const topicChatId = canonicalInteger(value, 'ETM TopicAssoc topic_chat_id')
  const parsed = BigInt(topicChatId)
  if (parsed <= -(BOT_API_CHANNEL_MARK + 1n)) {
    return { topicChatId, sourceChatId: (-parsed - BOT_API_CHANNEL_MARK).toString(), expectedPeer: 'channel' }
  }
  if (parsed < 0n)
    return { topicChatId, sourceChatId: (-parsed).toString(), expectedPeer: 'chat' }
  throw new Error(`ETM TopicAssoc group ID must be a negative Telegram Bot API chat ID: ${topicChatId}`)
}

export function normalizeBindings(rows: Array<Record<string, unknown>>): TopicBinding[] {
  const byTopic = new Map<string, TopicBinding>()
  const bySlave = new Map<string, string>()
  for (const row of rows) {
    const topicChatId = canonicalInteger(row.topic_chat_id, 'ETM TopicAssoc topic_chat_id')
    const messageThreadId = canonicalInteger(row.message_thread_id, 'ETM TopicAssoc message_thread_id')
    const slaveUid = String(row.slave_uid ?? '')
    if (!slaveUid)
      throw new Error('ETM TopicAssoc slave_uid must not be empty')
    parseEtmGroupId(topicChatId)
    const key = `${topicChatId}.${messageThreadId}`
    const existing = byTopic.get(key)
    if (existing && existing.slaveUid !== slaveUid)
      throw new Error(`Conflicting ETM TopicAssoc mapping for ${key}`)
    const slaveTarget = bySlave.get(slaveUid)
    if (slaveTarget && slaveTarget !== key)
      throw new Error(`Conflicting ETM TopicAssoc mapping for slave ${slaveUid}`)
    byTopic.set(key, { topicChatId, messageThreadId, slaveUid })
    bySlave.set(slaveUid, key)
  }
  const bindings = [...byTopic.values()].sort((a, b) => {
    const chat = BigInt(a.topicChatId) - BigInt(b.topicChatId)
    if (chat !== 0n)
      return chat < 0n ? -1 : 1
    const topic = BigInt(a.messageThreadId) - BigInt(b.messageThreadId)
    return topic < 0n ? -1 : topic > 0n ? 1 : a.slaveUid.localeCompare(b.slaveUid)
  })
  if (bindings.length === 0)
    throw new Error('ETM TopicAssoc contains no bound topics')
  return bindings
}

function normalizeMsgLogs(rows: Array<Record<string, unknown>>): MsgLogIdentity[] {
  return rows.map(row => ({
    primary: String(row.master_msg_id),
    alternate: row.master_msg_id_alt == null ? null : String(row.master_msg_id_alt),
    senderBotId: row.sender_bot_id == null ? null : canonicalInteger(row.sender_bot_id, 'MsgLog sender_bot_id'),
  })).sort((a, b) => a.primary.localeCompare(b.primary) || (a.alternate ?? '').localeCompare(b.alternate ?? ''))
}

function assertColumns(actual: string[], required: readonly string[], table: string) {
  const missing = required.filter(column => !actual.includes(column))
  if (missing.length)
    throw new Error(`Unsupported ETM schema: ${table} is missing ${missing.join(', ')}`)
}

function sqliteSnapshot(path: string, includeLogs: boolean): EtmSnapshot {
  const database = new DatabaseSync(path, { readOnly: true })
  try {
    const topicColumns = database.prepare('PRAGMA table_info(topicassoc)').all().map(row => String(row.name))
    const msgLogColumns = database.prepare('PRAGMA table_info(msglog)').all().map(row => String(row.name))
    assertColumns(topicColumns, TOPIC_COLUMNS, 'topicassoc')
    assertColumns(msgLogColumns, MSGLOG_COLUMNS, 'msglog')
    const bindings = normalizeBindings(database.prepare('SELECT topic_chat_id, message_thread_id, slave_uid FROM topicassoc').all() as Array<Record<string, unknown>>)
    if (!includeLogs)
      return { bindings, msgLogs: [] }
    const groups = [...new Set(bindings.map(binding => binding.topicChatId))]
    const predicate = groups.map(() => '(master_msg_id LIKE ? OR master_msg_id_alt LIKE ?)').join(' OR ')
    const parameters = groups.flatMap(group => [`${group}.%`, `${group}.%`])
    const rows = database.prepare(`SELECT master_msg_id, master_msg_id_alt, sender_bot_id FROM msglog WHERE ${predicate}`).all(...parameters)
    return { bindings, msgLogs: normalizeMsgLogs(rows as Array<Record<string, unknown>>) }
  }
  finally {
    database.close()
  }
}

async function postgresSnapshot(url: string, includeLogs: boolean): Promise<EtmSnapshot> {
  const client = new Client({ connectionString: url })
  await client.connect()
  try {
    await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
    const columns = await client.query<{ table_name: string, column_name: string }>(
      `SELECT table_name, column_name FROM information_schema.columns
       WHERE table_schema = current_schema() AND table_name IN ('topicassoc', 'msglog')`,
    )
    assertColumns(columns.rows.filter(row => row.table_name === 'topicassoc').map(row => row.column_name), TOPIC_COLUMNS, 'topicassoc')
    assertColumns(columns.rows.filter(row => row.table_name === 'msglog').map(row => row.column_name), MSGLOG_COLUMNS, 'msglog')
    const topicRows = await client.query('SELECT topic_chat_id, message_thread_id, slave_uid FROM topicassoc')
    const bindings = normalizeBindings(topicRows.rows)
    let msgLogs: MsgLogIdentity[] = []
    if (includeLogs) {
      const groups = [...new Set(bindings.map(binding => binding.topicChatId))]
      const rows = await client.query(
        `SELECT master_msg_id, master_msg_id_alt, sender_bot_id FROM msglog
         WHERE split_part(master_msg_id, '.', 1) = ANY($1::text[])
            OR split_part(COALESCE(master_msg_id_alt, ''), '.', 1) = ANY($1::text[])`,
        [groups],
      )
      msgLogs = normalizeMsgLogs(rows.rows)
    }
    await client.query('COMMIT')
    return { bindings, msgLogs }
  }
  catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
  finally {
    await client.end()
  }
}

export function readEtmSnapshot(source: RecoveryAuditInput['etm'], includeLogs: boolean): Promise<EtmSnapshot> {
  return source.backend === 'sqlite'
    ? Promise.resolve(sqliteSnapshot(source.path, includeLogs))
    : postgresSnapshot(source.url, includeLogs)
}

function sameBindings(left: TopicBinding[], right: TopicBinding[]): boolean {
  return JSON.stringify(left) === JSON.stringify(right)
}

function rawPeerId(peer: Api.TypePeer): string | undefined {
  if (peer instanceof Api.PeerUser)
    return peer.userId.toString()
  if (peer instanceof Api.PeerChat)
    return peer.chatId.toString()
  if (peer instanceof Api.PeerChannel)
    return peer.channelId.toString()
}

function messageOrder(a: AuditMessage, b: AuditMessage): number {
  const chat = BigInt(a.topicChatId) - BigInt(b.topicChatId)
  if (chat !== 0n)
    return chat < 0n ? -1 : 1
  if (a.timestamp !== b.timestamp)
    return a.timestamp - b.timestamp
  const message = BigInt(a.messageId) - BigInt(b.messageId)
  return message < 0n ? -1 : message > 0n ? 1 : 0
}

export function classifyAuditMessages(
  messages: AuditMessage[],
  snapshot: EtmSnapshot,
  verifiedBots: Set<string>,
): Array<AuditMessage & { identity: string, classification: DiagnosticClassification, nonImportable: true }> {
  const primary = new Set(snapshot.msgLogs.map(row => row.primary))
  const alternate = new Set(snapshot.msgLogs.flatMap(row => row.alternate ? [row.alternate] : []))
  const topics = new Set(snapshot.bindings.map(binding => `${binding.topicChatId}.${binding.messageThreadId}`))
  return messages.slice().sort(messageOrder).map((message) => {
    const identity = `${message.topicChatId}.${message.messageId}`
    let classification: DiagnosticClassification
    if (!verifiedBots.has(message.senderId))
      classification = 'human-or-unverified-sender'
    else if (!topics.has(`${message.topicChatId}.${message.topicId}`))
      classification = 'unbound-topic'
    else if (primary.has(identity))
      classification = 'present-primary'
    else if (alternate.has(identity))
      classification = 'present-alt'
    else
      classification = 'missing-at-snapshot'
    return { ...message, identity, classification, nonImportable: true }
  })
}

export function createRecoveryAuditService(options: {
  context: CoreContext
  logger: Logger
  entityService: Pick<EntityService, 'getInputPeer'>
  takeoutService: Pick<TakeoutService, 'takeoutMessages'>
  readSnapshot?: typeof readEtmSnapshot
}) {
  const { context, entityService, logger, takeoutService } = options
  const snapshotReader = options.readSnapshot ?? readEtmSnapshot

  return async function* auditRecovery(input: RecoveryAuditInput, signal?: AbortSignal): AsyncGenerator<RecoveryAuditUpdate> {
    const taskId = uuidv4()
    yield { type: 'started', taskId }
    const before = await snapshotReader(input.etm, false)
    const groups = [...new Set(before.bindings.map(binding => binding.topicChatId))]
      .map(parseEtmGroupId)
      .sort((a, b) => a.topicChatId.localeCompare(b.topicChatId))
    const messages = new Map<string, AuditMessage>()
    let audited = 0

    for (const group of groups) {
      if (signal?.aborted)
        throw signal.reason instanceof Error ? signal.reason : new DOMException('Recovery audit aborted', 'AbortError')
      const inputPeer = await entityService.getInputPeer(group.topicChatId)
      const entity = await context.getClient().getEntity(inputPeer)
      if (group.expectedPeer === 'channel' && (!(entity instanceof Api.Channel) || !entity.megagroup || entity.id.toString() !== group.sourceChatId))
        throw new Error(`ETM group ${group.topicChatId} did not resolve to its expected supergroup`)
      if (group.expectedPeer === 'chat' && (!(entity instanceof Api.Chat) || entity.id.toString() !== group.sourceChatId))
        throw new Error(`ETM group ${group.topicChatId} did not resolve to its expected basic group`)

      const task = createTask('takeout', { chatIds: [group.sourceChatId] }, context.emitter, logger)
      const abortTask = () => task.abort()
      signal?.addEventListener('abort', abortTask, { once: true })
      try {
        for await (const message of takeoutService.takeoutMessages(group.sourceChatId, {
          pagination: { limit: 100, offset: 0 },
          inputPeer,
          startTime: input.fromMs,
          endTime: input.toMs - 1,
          skipMedia: true,
          expectedCount: 0,
          disableAutoProgress: true,
          takeoutConsent: input.takeout,
          task,
        })) {
          const timestamp = message.date * 1000
          if (timestamp < input.fromMs || timestamp >= input.toMs)
            continue
          if (rawPeerId(message.peerId) !== group.sourceChatId)
            throw new Error(`Telegram returned message ${message.id} from an unexpected group`)
          const senderId = message.fromId ? rawPeerId(message.fromId) : undefined
          if (!senderId)
            continue
          const messageId = message.id.toString()
          const topicId = message.replyTo instanceof Api.MessageReplyHeader
            ? (message.replyTo.replyToTopId ?? message.replyTo.replyToMsgId)?.toString() ?? messageId
            : messageId
          messages.set(`${group.topicChatId}.${messageId}`, {
            topicChatId: group.topicChatId,
            sourceChatId: group.sourceChatId,
            messageId,
            senderId,
            timestamp: message.date,
            topicId,
          })
        }
      }
      finally {
        signal?.removeEventListener('abort', abortTask)
      }
      if (task.state.lastError)
        throw task.state.rawError ?? new Error(task.state.lastError)
      audited = messages.size
      yield { type: 'progress', taskId, topicChatId: group.topicChatId, sourceChatId: group.sourceChatId, audited }
    }

    const comparison = await snapshotReader(input.etm, true)
    if (!sameBindings(before.bindings, comparison.bindings))
      throw new Error('ETM TopicAssoc mappings changed during Telegram acquisition; audit aborted')

    const identities = messages
    const botCandidates = new Set(comparison.msgLogs.flatMap((row) => {
      const matched = identities.get(row.primary) ?? (row.alternate ? identities.get(row.alternate) : undefined)
      return [row.senderBotId, matched?.senderId].filter((value): value is string => Boolean(value))
    }))
    const verifiedBots = new Set<string>()
    for (const senderId of [...botCandidates].sort((a, b) => BigInt(a) < BigInt(b) ? -1 : 1)) {
      const entity = await context.getClient().getEntity(senderId)
      if (entity instanceof Api.User && entity.bot && entity.id.toString() === senderId)
        verifiedBots.add(senderId)
    }

    const diagnostics = classifyAuditMessages([...messages.values()], comparison, verifiedBots)
    const counts: Record<DiagnosticClassification, number> = {
      'present-primary': 0,
      'present-alt': 0,
      'missing-at-snapshot': 0,
      'unbound-topic': 0,
      'human-or-unverified-sender': 0,
    }
    for (const row of diagnostics)
      counts[row.classification] += 1
    const report = [{
      type: 'audit-summary',
      version: 1,
      purpose: 'read-only diagnostic evidence',
      backend: input.etm.backend,
      window: { from: new Date(input.fromMs).toISOString(), to: new Date(input.toMs).toISOString(), semantics: '[from,to)' },
      groups: groups.map(group => group.topicChatId),
      verifiedBotSenderIds: [...verifiedBots].sort((a, b) => BigInt(a) < BigInt(b) ? -1 : 1),
      counts,
      audited: diagnostics.length,
    }, ...diagnostics.map(row => ({ type: 'diagnostic', version: 1, ...row }))]
    const temporaryPath = `${input.outputFile}.${taskId}.tmp`
    try {
      await mkdir(dirname(input.outputFile), { recursive: true })
      await writeFile(temporaryPath, `${report.map(row => JSON.stringify(row)).join('\n')}\n`, { mode: 0o600 })
      await rename(temporaryPath, input.outputFile)
    }
    catch (error) {
      await rm(temporaryPath, { force: true })
      throw error
    }
    yield { type: 'completed', taskId, file: basename(input.outputFile), audited: diagnostics.length }
  }
}
