import type { Logger } from '@guiiai/logg'
import type {
  RecoveryRepairCounts,
  RecoveryRepairInput,
  RecoveryRepairSummary,
  RecoveryRepairUpdate,
} from '@tg-search/protocol'

import type { CoreContext } from '../context'
import type { EntityService } from './entity'
import type { TakeoutService } from './takeout'

import process from 'node:process'

import { mkdir, rename, rm, writeFile } from 'node:fs/promises'
import { basename, dirname } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

import bigInt from 'big-integer'

import { RECOVERY_REPAIR_FROM_ISO } from '@tg-search/protocol'
import { Client } from 'pg'
import { Api } from 'telegram'
import { v4 as uuidv4 } from 'uuid'

import { createTask } from '../utils/task'

const BOT_API_CHANNEL_MARK = 1_000_000_000_000n
const TOPIC_COLUMNS = ['topic_chat_id', 'message_thread_id', 'slave_uid'] as const
const MSGLOG_COLUMNS = [
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
] as const

export interface TopicBinding {
  topicChatId: string
  messageThreadId: string
  slaveUid: string
  slaveModule: string
}

export interface EtmInspection {
  bindings: TopicBinding[]
  senderEvidence: SenderEvidence[]
}

export interface SenderEvidence {
  primaryIdentity: string
  alternateIdentity: string | null
  senderBotId: string | null
}

export interface RepairCandidate {
  identity: string
  topicChatId: string
  messageId: string
  senderId: string
  senderBotId: string | null
  timestamp: number
  text: string
  binding: TopicBinding
}

export interface AcquiredMessage {
  topicChatId: string
  sourceChatId: string
  messageId: string
  senderId?: string
  timestamp: number
  text: string
  topicId?: string
}

type EtmSource = RecoveryRepairInput['etm']
type Presence = 'primary' | 'alternate' | 'missing'

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
  if (parsed <= -(BOT_API_CHANNEL_MARK + 1n))
    return { topicChatId, sourceChatId: (-parsed - BOT_API_CHANNEL_MARK).toString(), expectedPeer: 'channel' }
  if (parsed < 0n)
    return { topicChatId, sourceChatId: (-parsed).toString(), expectedPeer: 'chat' }
  throw new Error(`ETM TopicAssoc group ID must be a negative Telegram Bot API chat ID: ${topicChatId}`)
}

function parseSlaveModule(slaveUid: string): string {
  const parts = slaveUid.split(' ')
  if ((parts.length !== 2 && parts.length !== 3) || parts.some(part => part.length === 0))
    throw new Error(`Unusable ETM TopicAssoc slave_uid: ${slaveUid}`)
  return parts[0]
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
    const slaveModule = parseSlaveModule(slaveUid)
    const key = `${topicChatId}.${messageThreadId}`
    const existing = byTopic.get(key)
    if (existing && existing.slaveUid !== slaveUid)
      throw new Error(`Conflicting ETM TopicAssoc mapping for ${key}`)
    const slaveTarget = bySlave.get(slaveUid)
    if (slaveTarget && slaveTarget !== key)
      throw new Error(`Conflicting ETM TopicAssoc mapping for slave ${slaveUid}`)
    byTopic.set(key, { topicChatId, messageThreadId, slaveUid, slaveModule })
    bySlave.set(slaveUid, key)
  }
  const bindings = [...byTopic.values()].sort(compareBindings)
  if (bindings.length === 0)
    throw new Error('ETM TopicAssoc contains no bound topics')
  return bindings
}

function compareBindings(a: TopicBinding, b: TopicBinding): number {
  const chat = BigInt(a.topicChatId) - BigInt(b.topicChatId)
  if (chat !== 0n)
    return chat < 0n ? -1 : 1
  const topic = BigInt(a.messageThreadId) - BigInt(b.messageThreadId)
  return topic < 0n ? -1 : topic > 0n ? 1 : a.slaveUid.localeCompare(b.slaveUid)
}

function assertColumns(actual: string[], required: readonly string[], table: string) {
  const missing = required.filter(column => !actual.includes(column))
  if (missing.length)
    throw new Error(`Unsupported ETM schema: ${table} is missing ${missing.join(', ')}`)
}

function inspectSqlite(path: string): EtmInspection {
  const database = new DatabaseSync(path, { readOnly: true })
  try {
    const topicColumns = database.prepare('PRAGMA table_info(topicassoc)').all().map(row => String(row.name))
    const msgLogColumns = database.prepare('PRAGMA table_info(msglog)').all().map(row => String(row.name))
    assertColumns(topicColumns, TOPIC_COLUMNS, 'topicassoc')
    assertColumns(msgLogColumns, MSGLOG_COLUMNS, 'msglog')
    return {
      bindings: normalizeBindings(database.prepare('SELECT topic_chat_id, message_thread_id, slave_uid FROM topicassoc').all() as Array<Record<string, unknown>>),
      senderEvidence: normalizeSenderEvidence(database.prepare('SELECT master_msg_id, master_msg_id_alt, sender_bot_id FROM msglog').all() as Array<Record<string, unknown>>),
    }
  }
  finally {
    database.close()
  }
}

async function inspectPostgres(url: string): Promise<EtmInspection> {
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
    const rows = await client.query('SELECT topic_chat_id, message_thread_id, slave_uid FROM topicassoc')
    const senderRows = await client.query('SELECT master_msg_id, master_msg_id_alt, sender_bot_id FROM msglog')
    await client.query('COMMIT')
    return { bindings: normalizeBindings(rows.rows), senderEvidence: normalizeSenderEvidence(senderRows.rows) }
  }
  catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
  finally {
    await client.end()
  }
}

export function inspectEtm(source: EtmSource): Promise<EtmInspection> {
  return source.backend === 'sqlite'
    ? Promise.resolve(inspectSqlite(source.path))
    : inspectPostgres(source.url)
}

function sameBindings(left: TopicBinding[], right: TopicBinding[]): boolean {
  return JSON.stringify(left) === JSON.stringify(right)
}

export function normalizeSenderEvidence(rows: Array<Record<string, unknown>>): SenderEvidence[] {
  return rows.map(row => ({
    primaryIdentity: String(row.master_msg_id),
    alternateIdentity: row.master_msg_id_alt == null ? null : String(row.master_msg_id_alt),
    senderBotId: row.sender_bot_id == null ? null : canonicalInteger(row.sender_bot_id, 'ETM MsgLog sender_bot_id'),
  }))
}

function archiveIdentity(value: string, groups: Set<string>): string | undefined {
  const separator = value.lastIndexOf('.')
  if (separator <= 0)
    return undefined
  try {
    const chatId = canonicalInteger(value.slice(0, separator), 'ETM MsgLog chat ID')
    const messageId = canonicalInteger(value.slice(separator + 1), 'ETM MsgLog message ID')
    return groups.has(chatId) && BigInt(messageId) > 0n ? `${chatId}.${messageId}` : undefined
  }
  catch {
    return undefined
  }
}

export function inferSenderRoles(evidence: SenderEvidence[], messages: Map<string, AcquiredMessage>, groupIds: string[]): Map<string, string | null> {
  const groups = new Set(groupIds)
  const roles = new Map<string, string | null>()
  const addRole = (senderId: string, role: string | null) => {
    const existing = roles.get(senderId)
    if (roles.has(senderId) && existing !== role)
      throw new Error(`Contradictory main and auxiliary ETM sender evidence for Telegram user ${senderId}`)
    roles.set(senderId, role)
  }
  for (const row of evidence) {
    const archived = [row.primaryIdentity, row.alternateIdentity]
      .filter((identity): identity is string => identity !== null)
      .map(identity => archiveIdentity(identity, groups))
      .filter((identity): identity is string => identity !== undefined)
      .map(identity => messages.get(identity))
      .filter((message): message is AcquiredMessage => message !== undefined)
    const senders = [...new Set(archived.map(message => message.senderId).filter((id): id is string => id !== undefined))]
    if (senders.length > 1)
      throw new Error(`Existing ETM MsgLog identities contradict archive sender evidence for ${row.primaryIdentity}`)
    if (row.senderBotId !== null) {
      if (senders.length === 1 && senders[0] !== row.senderBotId)
        throw new Error(`ETM auxiliary sender evidence contradicts archive sender for ${row.primaryIdentity}`)
      addRole(row.senderBotId, row.senderBotId)
    }
    else if (senders.length === 1) {
      addRole(senders[0], null)
    }
  }
  return roles
}

async function resolveBot(client: { getEntity: (entity: Api.TypePeer) => Promise<unknown> }, id: string): Promise<boolean> {
  try {
    const entity = await client.getEntity(new Api.PeerUser({ userId: bigInt(id) }))
    return entity instanceof Api.User && entity.bot === true && entity.id.toString() === id
  }
  catch {
    return false
  }
}

function rawPeerId(peer: Api.TypePeer): string | undefined {
  if (peer instanceof Api.PeerUser)
    return peer.userId.toString()
  if (peer instanceof Api.PeerChat)
    return peer.chatId.toString()
  if (peer instanceof Api.PeerChannel)
    return peer.channelId.toString()
}

function compareMessages(a: AcquiredMessage, b: AcquiredMessage): number {
  const chat = BigInt(a.topicChatId) - BigInt(b.topicChatId)
  if (chat !== 0n)
    return chat < 0n ? -1 : 1
  if (a.timestamp !== b.timestamp)
    return a.timestamp - b.timestamp
  const message = BigInt(a.messageId) - BigInt(b.messageId)
  return message < 0n ? -1 : message > 0n ? 1 : 0
}

function sqlitePresence(database: DatabaseSync, identity: string): Presence {
  const row = database.prepare(
    'SELECT master_msg_id, master_msg_id_alt FROM msglog WHERE master_msg_id = ? OR master_msg_id_alt = ? LIMIT 1',
  ).get(identity, identity)
  if (!row)
    return 'missing'
  return String(row.master_msg_id) === identity ? 'primary' : 'alternate'
}

async function postgresPresences(client: Client, identities: string[]): Promise<Map<string, Presence>> {
  if (identities.length === 0)
    return new Map()
  const rows = await client.query<{ master_msg_id: string, master_msg_id_alt: string | null }>(
    `SELECT master_msg_id, master_msg_id_alt FROM msglog
     WHERE master_msg_id = ANY($1::text[]) OR master_msg_id_alt = ANY($1::text[])`,
    [identities],
  )
  const result = new Map<string, Presence>()
  for (const row of rows.rows) {
    if (identities.includes(row.master_msg_id))
      result.set(row.master_msg_id, 'primary')
    if (row.master_msg_id_alt && identities.includes(row.master_msg_id_alt))
      result.set(row.master_msg_id_alt, 'alternate')
  }
  return result
}

const INSERT_COLUMNS = MSGLOG_COLUMNS.join(', ')
const INSERT_VALUES = MSGLOG_COLUMNS.map(() => '?').join(', ')
const POSTGRES_INSERT_SQL = `INSERT INTO msglog (
  master_msg_id, master_msg_id_alt, slave_message_id, text, slave_origin_uid,
  slave_origin_display_name, slave_member_uid, slave_member_display_name, media_type,
  mime, file_id, file_unique_id, msg_type, pickle, sent_to, sender_bot_id, time
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
ON CONFLICT DO NOTHING`

function rowValues(candidate: RepairCandidate): Array<string | null> {
  const timestamp = new Date(candidate.timestamp * 1000).toISOString().replace('T', ' ').replace('Z', '')
  return [
    candidate.identity,
    null,
    `mtproto-backfill:${candidate.identity}`,
    candidate.text,
    candidate.binding.slaveUid,
    null,
    `${candidate.binding.slaveModule} __self__`,
    null,
    'Text',
    null,
    null,
    null,
    'Text',
    null,
    'blueset.telegram',
    candidate.senderBotId,
    timestamp,
  ]
}

export async function readInitialPresences(source: EtmSource, identities: string[]): Promise<Map<string, Presence>> {
  if (identities.length === 0)
    return new Map()
  if (source.backend === 'sqlite') {
    const database = new DatabaseSync(source.path, { readOnly: true })
    try {
      return new Map(identities.map(identity => [identity, sqlitePresence(database, identity)]))
    }
    finally {
      database.close()
    }
  }
  const client = new Client({ connectionString: source.url })
  await client.connect()
  try {
    await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
    const result = await postgresPresences(client, identities)
    await client.query('COMMIT')
    return result
  }
  catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
  finally {
    await client.end()
  }
}

interface InsertOutcome { inserted: number, concurrent: number, conflicts: number, errors: number }

function insertSqliteChunk(path: string, chunk: RepairCandidate[]): InsertOutcome {
  const database = new DatabaseSync(path)
  let transaction = false
  try {
    database.exec('BEGIN IMMEDIATE')
    transaction = true
    const insert = database.prepare(`INSERT OR IGNORE INTO msglog (${INSERT_COLUMNS}) VALUES (${INSERT_VALUES})`)
    let inserted = 0
    let concurrent = 0
    let conflicts = 0
    for (const candidate of chunk) {
      if (sqlitePresence(database, candidate.identity) !== 'missing') {
        concurrent += 1
        continue
      }
      const result = insert.run(...rowValues(candidate))
      if (result.changes === 1)
        inserted += 1
      else
        conflicts += 1
    }
    database.exec('COMMIT')
    transaction = false
    return { inserted, concurrent, conflicts, errors: 0 }
  }
  catch {
    if (transaction)
      database.exec('ROLLBACK')
    return { inserted: 0, concurrent: 0, conflicts: 0, errors: chunk.length }
  }
  finally {
    database.close()
  }
}

async function insertPostgresChunk(url: string, chunk: RepairCandidate[]): Promise<InsertOutcome> {
  const client = new Client({ connectionString: url })
  await client.connect()
  try {
    await client.query('BEGIN')
    await client.query('LOCK TABLE msglog IN SHARE ROW EXCLUSIVE MODE')
    const presences = await postgresPresences(client, chunk.map(candidate => candidate.identity))
    let inserted = 0
    let concurrent = 0
    let conflicts = 0
    for (const candidate of chunk) {
      if ((presences.get(candidate.identity) ?? 'missing') !== 'missing') {
        concurrent += 1
        continue
      }
      const result = await client.query(POSTGRES_INSERT_SQL, rowValues(candidate))
      if (result.rowCount === 1)
        inserted += 1
      else
        conflicts += 1
    }
    await client.query('COMMIT')
    return { inserted, concurrent, conflicts, errors: 0 }
  }
  catch {
    await client.query('ROLLBACK').catch(() => {})
    return { inserted: 0, concurrent: 0, conflicts: 0, errors: chunk.length }
  }
  finally {
    await client.end()
  }
}

export async function insertRepairCandidates(source: EtmSource, candidates: RepairCandidate[], chunkSize: number): Promise<InsertOutcome> {
  const total: InsertOutcome = { inserted: 0, concurrent: 0, conflicts: 0, errors: 0 }
  for (let index = 0; index < candidates.length; index += chunkSize) {
    const chunk = candidates.slice(index, index + chunkSize)
    const outcome = source.backend === 'sqlite'
      ? insertSqliteChunk(source.path, chunk)
      : await insertPostgresChunk(source.url, chunk)
    total.inserted += outcome.inserted
    total.concurrent += outcome.concurrent
    total.conflicts += outcome.conflicts
    total.errors += outcome.errors
  }
  return total
}

async function writeReport(
  path: string,
  summary: RecoveryRepairSummary,
  candidates: RepairCandidate[],
  presences: Map<string, Presence>,
) {
  const records = [
    { type: 'repair-summary', ...summary },
    ...candidates.map(candidate => ({
      type: 'repair-message',
      version: 1,
      identity: candidate.identity,
      topicChatId: candidate.topicChatId,
      messageId: candidate.messageId,
      senderBotId: candidate.senderBotId,
      topicId: candidate.binding.messageThreadId,
      status: presences.get(candidate.identity) === 'primary'
        ? 'present-primary'
        : presences.get(candidate.identity) === 'alternate'
          ? 'present-alt'
          : 'repair-attempted',
    })),
  ]
  const temporaryPath = `${path}.${process.pid}.tmp`
  try {
    await mkdir(dirname(path), { recursive: true })
    await writeFile(temporaryPath, `${records.map(row => JSON.stringify(row)).join('\n')}\n`, { mode: 0o600 })
    await rename(temporaryPath, path)
  }
  catch (error) {
    await rm(temporaryPath, { force: true })
    throw error
  }
}

function emptyCounts(): RecoveryRepairCounts {
  return {
    'present-primary': 0,
    'present-alt': 0,
    'inserted': 0,
    'unbound-topic': 0,
    'human-or-unverified-sender': 0,
    'unclassified-verified-bot': 0,
    'service-deleted-unusable': 0,
    'concurrent': 0,
    'conflicts': 0,
    'errors': 0,
  }
}

export function createRecoveryRepairService(options: {
  context: CoreContext
  logger: Logger
  entityService: Pick<EntityService, 'getInputPeer'>
  takeoutService: Pick<TakeoutService, 'takeoutMessages'>
  inspect?: typeof inspectEtm
  presences?: typeof readInitialPresences
  insert?: typeof insertRepairCandidates
}) {
  const { context, entityService, logger, takeoutService } = options
  const inspector = options.inspect ?? inspectEtm
  const presenceReader = options.presences ?? readInitialPresences
  const inserter = options.insert ?? insertRepairCandidates

  return async function* repairRecovery(input: RecoveryRepairInput, signal?: AbortSignal): AsyncGenerator<RecoveryRepairUpdate> {
    const fromMs = Date.parse(RECOVERY_REPAIR_FROM_ISO)
    const toMs = input.startedAtMs
    if (!Number.isFinite(toMs) || toMs <= fromMs)
      throw new Error(`Recovery repair clock must be later than ${RECOVERY_REPAIR_FROM_ISO}`)
    const taskId = uuidv4()
    yield { type: 'started', taskId }
    const before = await inspector(input.etm)
    const groups = [...new Set(before.bindings.map(binding => binding.topicChatId))]
      .map(parseEtmGroupId)
      .sort((a, b) => BigInt(a.topicChatId) < BigInt(b.topicChatId) ? -1 : 1)
    const messages = new Map<string, AcquiredMessage>()
    const counts = emptyCounts()

    for (const group of groups) {
      if (signal?.aborted)
        throw signal.reason instanceof Error ? signal.reason : new DOMException('Recovery repair aborted', 'AbortError')
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
          startTime: fromMs,
          endTime: toMs - 1,
          skipMedia: true,
          expectedCount: 0,
          disableAutoProgress: true,
          takeoutConsent: input.takeout,
          task,
        })) {
          const timestamp = message.date * 1000
          if (timestamp < fromMs || timestamp >= toMs)
            continue
          if (rawPeerId(message.peerId) !== group.sourceChatId)
            throw new Error(`Telegram returned message ${message.id} from an unexpected group`)
          const messageId = message.id.toString()
          const senderId = message.fromId ? rawPeerId(message.fromId) : undefined
          const topicId = message.replyTo instanceof Api.MessageReplyHeader
            ? (message.replyTo.replyToTopId ?? message.replyTo.replyToMsgId)?.toString()
            : undefined
          messages.set(`${group.topicChatId}.${messageId}`, {
            topicChatId: group.topicChatId,
            sourceChatId: group.sourceChatId,
            messageId,
            senderId,
            timestamp: message.date,
            text: message.message,
            topicId,
          })
        }
      }
      finally {
        signal?.removeEventListener('abort', abortTask)
      }
      if (task.state.lastError)
        throw task.state.rawError ?? new Error(task.state.lastError)
      yield { type: 'progress', taskId, topicChatId: group.topicChatId, sourceChatId: group.sourceChatId, examined: messages.size }
    }

    const after = await inspector(input.etm)
    if (!sameBindings(before.bindings, after.bindings))
      throw new Error('ETM TopicAssoc mappings changed during Telegram acquisition; repair aborted')

    const roles = inferSenderRoles(after.senderEvidence, messages, groups.map(group => group.topicChatId))
    for (const senderId of [...roles.keys()].sort((a, b) => BigInt(a) < BigInt(b) ? -1 : 1)) {
      if (!await resolveBot(context.getClient(), senderId))
        throw new Error(`ETM sender evidence ${senderId} did not resolve to a Telegram bot user`)
    }
    const unknownSenderIsBot = new Map<string, boolean>()
    const bindings = new Map(after.bindings.map(binding => [`${binding.topicChatId}.${binding.messageThreadId}`, binding]))
    const candidates: RepairCandidate[] = []
    for (const message of [...messages.values()].sort(compareMessages)) {
      if (!message.senderId || !message.topicId || !message.text.trim()) {
        counts['service-deleted-unusable'] += 1
        continue
      }
      const binding = bindings.get(`${message.topicChatId}.${message.topicId}`)
      if (!binding) {
        counts['unbound-topic'] += 1
        continue
      }
      if (!roles.has(message.senderId)) {
        let verifiedBot = unknownSenderIsBot.get(message.senderId)
        if (verifiedBot === undefined) {
          verifiedBot = await resolveBot(context.getClient(), message.senderId)
          unknownSenderIsBot.set(message.senderId, verifiedBot)
        }
        counts[verifiedBot ? 'unclassified-verified-bot' : 'human-or-unverified-sender'] += 1
        continue
      }
      candidates.push({
        identity: `${message.topicChatId}.${message.messageId}`,
        topicChatId: message.topicChatId,
        messageId: message.messageId,
        senderId: message.senderId,
        senderBotId: roles.get(message.senderId) ?? null,
        timestamp: message.timestamp,
        text: message.text,
        binding,
      })
    }

    const presences = await presenceReader(input.etm, candidates.map(candidate => candidate.identity))
    const missing: RepairCandidate[] = []
    for (const candidate of candidates) {
      const presence = presences.get(candidate.identity) ?? 'missing'
      if (presence === 'primary')
        counts['present-primary'] += 1
      else if (presence === 'alternate')
        counts['present-alt'] += 1
      else
        missing.push(candidate)
    }

    const outcome = await inserter(input.etm, missing, input.chunkSize)
    counts.inserted = outcome.inserted
    counts.concurrent = outcome.concurrent
    counts.conflicts = outcome.conflicts
    counts.errors = outcome.errors
    const summary: RecoveryRepairSummary = {
      version: 1,
      backend: input.etm.backend,
      window: { from: RECOVERY_REPAIR_FROM_ISO, to: new Date(toMs).toISOString(), semantics: '[from,to)' },
      groups: groups.map(group => group.topicChatId),
      mainBotIds: [...roles].filter(([, role]) => role === null).map(([id]) => id).sort(),
      auxiliaryBotIds: [...roles].filter(([, role]) => role !== null).map(([id]) => id).sort(),
      counts,
      examined: messages.size,
    }
    if (input.outputFile)
      await writeReport(input.outputFile, summary, candidates, presences)
    yield { type: 'completed', summary, file: input.outputFile ? basename(input.outputFile) : null }
  }
}
