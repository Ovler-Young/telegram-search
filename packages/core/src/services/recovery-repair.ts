import type { Logger } from '@guiiai/logg'
import type {
  RecoveryRepairCounts,
  RecoveryRepairInput,
  RecoveryRepairNameSource,
  RecoveryRepairSlaveCounts,
  RecoveryRepairSummary,
  RecoveryRepairUpdate,
} from '@tg-search/protocol'
import type { PoolClient } from 'pg'

import type { CoreContext } from '../context'
import type { EntityService } from './entity'
import type { TakeoutService } from './takeout'

import { mkdir, open } from 'node:fs/promises'
import { basename, dirname } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

import { RECOVERY_REPAIR_FROM_ISO } from '@tg-search/protocol'
import { Pool } from 'pg'
import { Api } from 'telegram'
import { v4 as uuidv4 } from 'uuid'

import { createTask } from '../utils/task'

const BOT_API_CHANNEL_MARK = 1_000_000_000_000n
const MAX_SIGNED_64 = 9_223_372_036_854_775_807n
const TOPIC_COLUMNS = ['topic_chat_id', 'message_thread_id', 'slave_uid'] as const
const SLAVE_CHAT_INFO_COLUMNS = ['id', 'slave_channel_id', 'slave_chat_uid', 'slave_chat_group_id', 'slave_chat_name'] as const
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

export interface SlaveDisplayName {
  slaveUid: string
  slaveName: string
  nameSource: RecoveryRepairNameSource
}

export interface EtmInspection {
  bindings: TopicBinding[]
  slaveNames?: Map<string, SlaveDisplayName>
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

type UnavailableBoundGroupCategory
  = | 'missing-input-entity'
    | 'channel-invalid'
    | 'channel-private'
    | 'channel-public-group-na'
    | 'chat-id-invalid'
    | 'peer-id-invalid'
    | 'user-not-participant'

interface UnavailableBoundGroup {
  topicChatId: string
  sourceChatId: string
  category: UnavailableBoundGroupCategory
}

type ReportCandidateStatus = 'present-primary' | 'present-alt' | 'repair-attempted'

type EtmSource = RecoveryRepairInput['etm']
type Presence = 'primary' | 'alternate' | 'missing'
type InsertStatus = 'inserted' | 'concurrent' | 'conflict' | 'error'

function canonicalInteger(value: unknown, label: string): string {
  const text = String(value)
  if (!/^-?\d+$/.test(text))
    throw new Error(`Invalid ${label}: ${text}`)
  const canonical = BigInt(text).toString()
  if (canonical === '0')
    throw new Error(`${label} must not be zero`)
  return canonical
}

function configuredSenderRoles(mainBotId: string, auxiliaryBotIds: string[]): Map<string, string | null> {
  const normalize = (value: string) => {
    if (!/^[1-9]\d*$/.test(value))
      throw new Error('Configured ETM bot IDs must be canonical positive decimal integers')
    const id = BigInt(value)
    if (id > MAX_SIGNED_64)
      throw new Error('Configured ETM bot IDs must fit in the signed 64-bit range')
    return id.toString()
  }
  const main = normalize(mainBotId)
  const roles = new Map<string, string | null>([[main, null]])
  for (const value of auxiliaryBotIds) {
    const id = normalize(value)
    if (roles.has(id))
      throw new Error('Configured ETM bot IDs must be unique across main and auxiliary roles')
    roles.set(id, id)
  }
  return roles
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
  return parseSlaveUid(slaveUid).module
}

function parseSlaveUid(slaveUid: string): { module: string, chatUid: string, groupId: string | null } {
  const parts = slaveUid.split(' ')
  if ((parts.length !== 2 && parts.length !== 3) || parts.some(part => part.length === 0))
    throw new Error(`Unusable ETM TopicAssoc slave_uid: ${slaveUid}`)
  return { module: parts[0], chatUid: parts[1], groupId: parts[2] ?? null }
}

function fallbackSlaveName(slaveUid: string): SlaveDisplayName {
  return { slaveUid, slaveName: slaveUid, nameSource: 'slave_uid' }
}

function nonemptyName(value: unknown): string | null {
  const name = String(value ?? '').trim()
  return name || null
}

export function normalizeBindings(rows: Array<Record<string, unknown>>): TopicBinding[] {
  const byTopic = new Map<string, TopicBinding>()
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
    byTopic.set(key, { topicChatId, messageThreadId, slaveUid, slaveModule })
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

function hasColumns(actual: string[], required: readonly string[]): boolean {
  return required.every(column => actual.includes(column))
}

function sqliteColumns(database: DatabaseSync, table: string): string[] {
  return database.prepare(`PRAGMA table_info(${table})`).all().map(row => String(row.name))
}

function sqliteSlaveDisplayName(database: DatabaseSync, slaveUid: string, hasSlaveChatInfo: boolean): SlaveDisplayName {
  const parsed = parseSlaveUid(slaveUid)
  if (hasSlaveChatInfo) {
    const row = database.prepare(`
      SELECT slave_chat_name FROM slavechatinfo
      WHERE slave_channel_id = ?
        AND slave_chat_uid = ?
        AND ((? IS NULL AND slave_chat_group_id IS NULL) OR slave_chat_group_id = ?)
        AND slave_chat_name IS NOT NULL
        AND trim(slave_chat_name) <> ''
      ORDER BY id DESC
      LIMIT 1
    `).get(parsed.module, parsed.chatUid, parsed.groupId, parsed.groupId) as { slave_chat_name?: unknown } | undefined
    const slaveName = nonemptyName(row?.slave_chat_name)
    if (slaveName)
      return { slaveUid, slaveName, nameSource: 'slavechatinfo.slave_chat_name' }
  }

  const legacy = database.prepare(`
    SELECT slave_origin_display_name FROM msglog
    WHERE slave_origin_uid = ?
      AND slave_origin_display_name IS NOT NULL
      AND trim(slave_origin_display_name) <> ''
    ORDER BY time IS NULL ASC, time DESC, master_msg_id DESC
    LIMIT 1
  `).get(slaveUid) as { slave_origin_display_name?: unknown } | undefined
  const legacyName = nonemptyName(legacy?.slave_origin_display_name)
  return legacyName
    ? { slaveUid, slaveName: legacyName, nameSource: 'msglog.slave_origin_display_name' }
    : fallbackSlaveName(slaveUid)
}

function sqliteSlaveDisplayNames(database: DatabaseSync, bindings: TopicBinding[]): Map<string, SlaveDisplayName> {
  const slaveColumns = sqliteColumns(database, 'slavechatinfo')
  const hasSlaveChatInfo = hasColumns(slaveColumns, SLAVE_CHAT_INFO_COLUMNS)
  return new Map([...new Set(bindings.map(binding => binding.slaveUid))]
    .sort()
    .map(slaveUid => [slaveUid, sqliteSlaveDisplayName(database, slaveUid, hasSlaveChatInfo)]))
}

function inspectSqlite(path: string): EtmInspection {
  const database = new DatabaseSync(path, { readOnly: true })
  try {
    const topicColumns = sqliteColumns(database, 'topicassoc')
    const msgLogColumns = sqliteColumns(database, 'msglog')
    assertColumns(topicColumns, TOPIC_COLUMNS, 'topicassoc')
    assertColumns(msgLogColumns, MSGLOG_COLUMNS, 'msglog')
    const bindings = normalizeBindings(database.prepare('SELECT topic_chat_id, message_thread_id, slave_uid FROM topicassoc').all() as Array<Record<string, unknown>>)
    return {
      bindings,
      slaveNames: sqliteSlaveDisplayNames(database, bindings),
    }
  }
  finally {
    database.close()
  }
}

export function postgresPoolConfig(source: Extract<EtmSource, { backend: 'postgres' }>) {
  return {
    database: source.database,
    host: source.host,
    port: source.port,
    user: source.user,
    password: source.password,
    max: source.maxConnections,
    options: source.options,
  }
}

function createPostgresPool(source: Extract<EtmSource, { backend: 'postgres' }>): Pool {
  return new Pool(postgresPoolConfig(source))
}

async function postgresSlaveDisplayName(client: PoolClient, slaveUid: string, hasSlaveChatInfo: boolean): Promise<SlaveDisplayName> {
  const parsed = parseSlaveUid(slaveUid)
  if (hasSlaveChatInfo) {
    const rows = await client.query<{ slave_chat_name: string }>(
      `SELECT slave_chat_name FROM slavechatinfo
       WHERE slave_channel_id = $1
         AND slave_chat_uid = $2
         AND (($3::text IS NULL AND slave_chat_group_id IS NULL) OR slave_chat_group_id = $3)
         AND slave_chat_name IS NOT NULL
         AND trim(slave_chat_name) <> ''
       ORDER BY id DESC
       LIMIT 1`,
      [parsed.module, parsed.chatUid, parsed.groupId],
    )
    const slaveName = nonemptyName(rows.rows[0]?.slave_chat_name)
    if (slaveName)
      return { slaveUid, slaveName, nameSource: 'slavechatinfo.slave_chat_name' }
  }

  const legacy = await client.query<{ slave_origin_display_name: string }>(
    `SELECT slave_origin_display_name FROM msglog
     WHERE slave_origin_uid = $1
       AND slave_origin_display_name IS NOT NULL
       AND trim(slave_origin_display_name) <> ''
     ORDER BY time IS NULL ASC, time DESC, master_msg_id DESC
     LIMIT 1`,
    [slaveUid],
  )
  const legacyName = nonemptyName(legacy.rows[0]?.slave_origin_display_name)
  return legacyName
    ? { slaveUid, slaveName: legacyName, nameSource: 'msglog.slave_origin_display_name' }
    : fallbackSlaveName(slaveUid)
}

async function postgresSlaveDisplayNames(client: PoolClient, bindings: TopicBinding[], hasSlaveChatInfo: boolean): Promise<Map<string, SlaveDisplayName>> {
  const slaveNames = new Map<string, SlaveDisplayName>()
  for (const slaveUid of [...new Set(bindings.map(binding => binding.slaveUid))].sort())
    slaveNames.set(slaveUid, await postgresSlaveDisplayName(client, slaveUid, hasSlaveChatInfo))
  return slaveNames
}

async function inspectPostgres(source: Extract<EtmSource, { backend: 'postgres' }>): Promise<EtmInspection> {
  const pool = createPostgresPool(source)
  const client = await pool.connect()
  try {
    await client.query('BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
    const columns = await client.query<{ table_name: string, column_name: string }>(
      `SELECT table_name, column_name FROM information_schema.columns
       WHERE table_schema = current_schema() AND table_name IN ('topicassoc', 'msglog', 'slavechatinfo')`,
    )
    assertColumns(columns.rows.filter(row => row.table_name === 'topicassoc').map(row => row.column_name), TOPIC_COLUMNS, 'topicassoc')
    assertColumns(columns.rows.filter(row => row.table_name === 'msglog').map(row => row.column_name), MSGLOG_COLUMNS, 'msglog')
    const rows = await client.query('SELECT topic_chat_id, message_thread_id, slave_uid FROM topicassoc')
    const bindings = normalizeBindings(rows.rows)
    const slaveNames = await postgresSlaveDisplayNames(
      client,
      bindings,
      hasColumns(columns.rows.filter(row => row.table_name === 'slavechatinfo').map(row => row.column_name), SLAVE_CHAT_INFO_COLUMNS),
    )
    await client.query('COMMIT')
    return { bindings, slaveNames }
  }
  catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
  finally {
    client.release()
    await pool.end()
  }
}

export function inspectEtm(source: EtmSource): Promise<EtmInspection> {
  return source.backend === 'sqlite'
    ? Promise.resolve(inspectSqlite(source.path))
    : inspectPostgres(source)
}

function sameBindings(left: TopicBinding[], right: TopicBinding[]): boolean {
  return JSON.stringify(left) === JSON.stringify(right)
}

function inspectionSlaveNames(inspection: EtmInspection): Map<string, SlaveDisplayName> {
  return inspection.slaveNames ?? new Map()
}

function rawPeerId(peer: Api.TypePeer): string | undefined {
  if (peer instanceof Api.PeerUser)
    return peer.userId.toString()
  if (peer instanceof Api.PeerChat)
    return peer.chatId.toString()
  if (peer instanceof Api.PeerChannel)
    return peer.channelId.toString()
}

function assertGroupInputPeer(group: ReturnType<typeof parseEtmGroupId>, inputPeer: Api.TypeInputPeer) {
  if (group.expectedPeer === 'channel') {
    if (!(inputPeer instanceof Api.InputPeerChannel) || inputPeer.channelId.toString() !== group.sourceChatId)
      throw new Error(`ETM group ${group.topicChatId} did not resolve to its expected supergroup input peer`)
    return
  }
  if (!(inputPeer instanceof Api.InputPeerChat) || inputPeer.chatId.toString() !== group.sourceChatId)
    throw new Error(`ETM group ${group.topicChatId} did not resolve to its expected basic group input peer`)
}

const UNAVAILABLE_RPC_ERRORS = new Map<string, UnavailableBoundGroupCategory>([
  ['CHANNEL_INVALID', 'channel-invalid'],
  ['CHANNEL_PRIVATE', 'channel-private'],
  ['CHANNEL_PUBLIC_GROUP_NA', 'channel-public-group-na'],
  ['CHAT_ID_INVALID', 'chat-id-invalid'],
  ['PEER_ID_INVALID', 'peer-id-invalid'],
  ['USER_NOT_PARTICIPANT', 'user-not-participant'],
])

function classifyUnavailableBoundGroup(error: unknown): UnavailableBoundGroupCategory | undefined {
  const message = error instanceof Error ? error.message : String(error)
  if (
    message.startsWith('Could not find the input entity for ')
    || message.startsWith('Cannot find any entity corresponding to ')
  ) {
    return 'missing-input-entity'
  }

  const rpcMessage = typeof error === 'object' && error && 'errorMessage' in error
    ? String((error as { errorMessage?: unknown }).errorMessage)
    : undefined
  if (!rpcMessage)
    return undefined

  return UNAVAILABLE_RPC_ERRORS.get(rpcMessage)
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

async function postgresPresences(client: PoolClient, identities: string[]): Promise<Map<string, Presence>> {
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
  const pool = createPostgresPool(source)
  const client = await pool.connect()
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
    client.release()
    await pool.end()
  }
}

interface InsertOutcome {
  inserted: number
  concurrent: number
  conflicts: number
  errors: number
  statuses?: Map<string, InsertStatus>
}

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
    const statuses = new Map<string, InsertStatus>()
    for (const candidate of chunk) {
      if (sqlitePresence(database, candidate.identity) !== 'missing') {
        concurrent += 1
        statuses.set(candidate.identity, 'concurrent')
        continue
      }
      const result = insert.run(...rowValues(candidate))
      if (result.changes === 1) {
        inserted += 1
        statuses.set(candidate.identity, 'inserted')
      }
      else {
        conflicts += 1
        statuses.set(candidate.identity, 'conflict')
      }
    }
    database.exec('COMMIT')
    transaction = false
    return { inserted, concurrent, conflicts, errors: 0, statuses }
  }
  catch (error) {
    if (transaction)
      database.exec('ROLLBACK')
    throw error
  }
  finally {
    database.close()
  }
}

async function insertPostgresChunk(source: Extract<EtmSource, { backend: 'postgres' }>, chunk: RepairCandidate[]): Promise<InsertOutcome> {
  const pool = createPostgresPool(source)
  const client = await pool.connect()
  try {
    await client.query('BEGIN')
    await client.query('LOCK TABLE msglog IN SHARE ROW EXCLUSIVE MODE')
    const presences = await postgresPresences(client, chunk.map(candidate => candidate.identity))
    let inserted = 0
    let concurrent = 0
    let conflicts = 0
    const statuses = new Map<string, InsertStatus>()
    for (const candidate of chunk) {
      if ((presences.get(candidate.identity) ?? 'missing') !== 'missing') {
        concurrent += 1
        statuses.set(candidate.identity, 'concurrent')
        continue
      }
      const result = await client.query(POSTGRES_INSERT_SQL, rowValues(candidate))
      if (result.rowCount === 1) {
        inserted += 1
        statuses.set(candidate.identity, 'inserted')
      }
      else {
        conflicts += 1
        statuses.set(candidate.identity, 'conflict')
      }
    }
    await client.query('COMMIT')
    return { inserted, concurrent, conflicts, errors: 0, statuses }
  }
  catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
  finally {
    client.release()
    await pool.end()
  }
}

export async function insertRepairCandidates(source: EtmSource, candidates: RepairCandidate[], chunkSize: number): Promise<InsertOutcome> {
  const total: InsertOutcome = { inserted: 0, concurrent: 0, conflicts: 0, errors: 0, statuses: new Map() }
  for (let index = 0; index < candidates.length; index += chunkSize) {
    const chunk = candidates.slice(index, index + chunkSize)
    const outcome = source.backend === 'sqlite'
      ? insertSqliteChunk(source.path, chunk)
      : await insertPostgresChunk(source, chunk)
    total.inserted += outcome.inserted
    total.concurrent += outcome.concurrent
    total.conflicts += outcome.conflicts
    total.errors += outcome.errors
    for (const [identity, status] of outcome.statuses ?? [])
      total.statuses?.set(identity, status)
  }
  return total
}

function emptyCounts(): RecoveryRepairCounts {
  return {
    'present-primary': 0,
    'present-alt': 0,
    'inserted': 0,
    'unavailable-bound-group': 0,
    'unbound-topic': 0,
    'human-or-unconfigured-sender': 0,
    'service-deleted-unusable': 0,
    'concurrent': 0,
    'conflicts': 0,
    'errors': 0,
  }
}

function addCounts(total: RecoveryRepairCounts, next: RecoveryRepairCounts) {
  for (const key of Object.keys(total) as Array<keyof RecoveryRepairCounts>)
    total[key] += next[key]
}

function emptySlaveCounts(): RecoveryRepairSlaveCounts {
  return {
    mappedExamined: 0,
    eligible: 0,
    presentPrimary: 0,
    presentAlt: 0,
    inserted: 0,
    concurrent: 0,
    conflicts: 0,
    errors: 0,
    skipped: {
      'human-or-unconfigured-sender': 0,
      'service-deleted-unusable': 0,
    },
  }
}

function slaveCounts(countsBySlave: Map<string, RecoveryRepairSlaveCounts>, slaveUid: string): RecoveryRepairSlaveCounts {
  const existing = countsBySlave.get(slaveUid)
  if (existing)
    return existing
  const counts = emptySlaveCounts()
  countsBySlave.set(slaveUid, counts)
  return counts
}

function addSlaveInsertStatus(counts: RecoveryRepairSlaveCounts, status: InsertStatus | undefined) {
  if (status === 'inserted')
    counts.inserted += 1
  else if (status === 'concurrent')
    counts.concurrent += 1
  else if (status === 'conflict')
    counts.conflicts += 1
  else if (status === 'error')
    counts.errors += 1
}

function bindingsForGroup(bindings: TopicBinding[], topicChatId: string): TopicBinding[] {
  return bindings.filter(binding => binding.topicChatId === topicChatId).sort(compareBindings)
}

function candidateStatus(candidate: RepairCandidate, presences: Map<string, Presence>): ReportCandidateStatus {
  const presence = presences.get(candidate.identity)
  if (presence === 'primary')
    return 'present-primary'
  if (presence === 'alternate')
    return 'present-alt'
  return 'repair-attempted'
}

function buildGroupCandidates(
  messages: Iterable<AcquiredMessage>,
  bindings: Map<string, TopicBinding>,
  roles: Map<string, string | null>,
): { candidates: RepairCandidate[], counts: RecoveryRepairCounts, slaveCounts: Map<string, RecoveryRepairSlaveCounts> } {
  const counts = emptyCounts()
  const countsBySlave = new Map<string, RecoveryRepairSlaveCounts>()
  const candidates: RepairCandidate[] = []
  for (const message of [...messages].sort(compareMessages)) {
    if (!message.topicId) {
      counts['service-deleted-unusable'] += 1
      continue
    }
    const binding = bindings.get(`${message.topicChatId}.${message.topicId}`)
    if (!binding) {
      counts['unbound-topic'] += 1
      continue
    }
    const perSlave = slaveCounts(countsBySlave, binding.slaveUid)
    perSlave.mappedExamined += 1
    if (!message.senderId || !message.text.trim()) {
      counts['service-deleted-unusable'] += 1
      perSlave.skipped['service-deleted-unusable'] += 1
      continue
    }
    if (!roles.has(message.senderId)) {
      counts['human-or-unconfigured-sender'] += 1
      perSlave.skipped['human-or-unconfigured-sender'] += 1
      continue
    }
    perSlave.eligible += 1
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
  return { candidates, counts, slaveCounts: countsBySlave }
}

function splitCandidatesByPresence(
  candidates: RepairCandidate[],
  presences: Map<string, Presence>,
  counts: RecoveryRepairCounts,
  countsBySlave: Map<string, RecoveryRepairSlaveCounts>,
): RepairCandidate[] {
  const missing: RepairCandidate[] = []
  for (const candidate of candidates) {
    const presence = presences.get(candidate.identity) ?? 'missing'
    const perSlave = slaveCounts(countsBySlave, candidate.binding.slaveUid)
    if (presence === 'primary') {
      counts['present-primary'] += 1
      perSlave.presentPrimary += 1
    }
    else if (presence === 'alternate') {
      counts['present-alt'] += 1
      perSlave.presentAlt += 1
    }
    else {
      missing.push(candidate)
    }
  }
  return missing
}

function applyInsertOutcome(counts: RecoveryRepairCounts, outcome: InsertOutcome) {
  counts.inserted = outcome.inserted
  counts.concurrent = outcome.concurrent
  counts.conflicts = outcome.conflicts
  counts.errors = outcome.errors
}

function applySlaveInsertOutcome(
  candidates: RepairCandidate[],
  outcome: InsertOutcome,
  countsBySlave: Map<string, RecoveryRepairSlaveCounts>,
) {
  for (const candidate of candidates)
    addSlaveInsertStatus(slaveCounts(countsBySlave, candidate.binding.slaveUid), outcome.statuses?.get(candidate.identity))
}

function reportCandidate(candidate: RepairCandidate, presences: Map<string, Presence>) {
  return {
    identity: candidate.identity,
    topicChatId: candidate.topicChatId,
    messageId: candidate.messageId,
    senderBotId: candidate.senderBotId,
    topicId: candidate.binding.messageThreadId,
    status: candidateStatus(candidate, presences),
  }
}

function compareSlaveIdentity(left: { slaveName: string, slaveUid: string }, right: { slaveName: string, slaveUid: string }): number {
  const name = left.slaveName.localeCompare(right.slaveName)
  return name || left.slaveUid.localeCompare(right.slaveUid)
}

function buildSlaveSummaryEvents(
  taskId: string,
  topicChatId: string,
  slaveNames: Map<string, SlaveDisplayName>,
  countsBySlave: Map<string, RecoveryRepairSlaveCounts>,
): Array<RecoveryRepairUpdate & { type: 'slave-summary' }> {
  return [...countsBySlave.entries()].map(([slaveUid, counts]) => {
    const displayName = slaveNames.get(slaveUid) ?? fallbackSlaveName(slaveUid)
    return {
      type: 'slave-summary',
      taskId,
      version: 2,
      topicChatId,
      slaveUid,
      slaveName: displayName.slaveName,
      nameSource: displayName.nameSource,
      counts,
    } as const
  }).sort(compareSlaveIdentity)
}

function unavailableBindingReports(
  bindings: TopicBinding[],
  slaveNames: Map<string, SlaveDisplayName>,
) {
  return bindings.map((binding) => {
    const displayName = slaveNames.get(binding.slaveUid) ?? fallbackSlaveName(binding.slaveUid)
    return {
      messageThreadId: binding.messageThreadId,
      slaveUid: binding.slaveUid,
      slaveName: displayName.slaveName,
      nameSource: displayName.nameSource,
    }
  }).sort((left, right) => {
    const identity = compareSlaveIdentity(left, right)
    if (identity)
      return identity
    const topic = BigInt(left.messageThreadId) - BigInt(right.messageThreadId)
    return topic < 0n ? -1 : topic > 0n ? 1 : 0
  })
}

async function appendReportEvent(path: string, event: Record<string, unknown>) {
  await mkdir(dirname(path), { recursive: true })
  const file = await open(path, 'a', 0o600)
  try {
    await file.chmod(0o600)
    await file.writeFile(`${JSON.stringify(event)}\n`)
    await file.sync()
  }
  finally {
    await file.close()
  }
}

function safeFailureCategory(error: unknown): string {
  if (error instanceof DOMException && error.name === 'AbortError')
    return 'aborted'
  const rpcMessage = typeof error === 'object' && error && 'errorMessage' in error
    ? String((error as { errorMessage?: unknown }).errorMessage)
    : undefined
  if (rpcMessage && /^[A-Z][A-Z0-9_]*$/.test(rpcMessage))
    return `telegram-${rpcMessage.toLowerCase().replaceAll('_', '-')}`
  return error instanceof Error && error.name ? error.name : 'error'
}

function buildSummary(
  input: RecoveryRepairInput,
  toMs: number,
  groups: Array<ReturnType<typeof parseEtmGroupId>>,
  roles: Map<string, string | null>,
  counts: RecoveryRepairCounts,
  examined: number,
): RecoveryRepairSummary {
  return {
    version: 1,
    backend: input.etm.backend,
    window: { from: RECOVERY_REPAIR_FROM_ISO, to: new Date(toMs).toISOString(), semantics: '[from,to)' },
    groups: groups.map(group => group.topicChatId),
    mainBotIds: [...roles].filter(([, role]) => role === null).map(([id]) => id).sort(),
    auxiliaryBotIds: [...roles].filter(([, role]) => role !== null).map(([id]) => id).sort(),
    counts,
    examined,
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
    const roles = configuredSenderRoles(input.mainBotId, input.auxiliaryBotIds)
    const counts = emptyCounts()
    let examined = 0
    let reportStarted = false
    const before = await inspector(input.etm)
    const groups = [...new Set(before.bindings.map(binding => binding.topicChatId))]
      .map(parseEtmGroupId)
      .sort((a, b) => BigInt(a.topicChatId) < BigInt(b.topicChatId) ? -1 : 1)

    try {
      if (input.outputFile) {
        await appendReportEvent(input.outputFile, {
          type: 'run-start',
          version: 2,
          runId: taskId,
          backend: input.etm.backend,
          window: { from: RECOVERY_REPAIR_FROM_ISO, to: new Date(toMs).toISOString(), semantics: '[from,to)' },
          groups: groups.map(group => group.topicChatId),
          mainBotIds: [...roles].filter(([, role]) => role === null).map(([id]) => id).sort(),
          auxiliaryBotIds: [...roles].filter(([, role]) => role !== null).map(([id]) => id).sort(),
        })
        reportStarted = true
      }

      for (const group of groups) {
        if (signal?.aborted)
          throw signal.reason instanceof Error ? signal.reason : new DOMException('Recovery repair aborted', 'AbortError')
        const groupMessages = new Map<string, AcquiredMessage>()
        const task = createTask('takeout', { chatIds: [group.sourceChatId] }, context.emitter, logger)
        const abortTask = () => task.abort()
        signal?.addEventListener('abort', abortTask, { once: true })
        try {
          const inputPeer = await entityService.getInputPeer(group.sourceChatId)
          assertGroupInputPeer(group, inputPeer)
          const entity = await context.getClient().getEntity(inputPeer)
          if (group.expectedPeer === 'channel' && (!(entity instanceof Api.Channel) || !entity.megagroup || entity.id.toString() !== group.sourceChatId))
            throw new Error(`ETM group ${group.topicChatId} did not resolve to its expected supergroup`)
          if (group.expectedPeer === 'chat' && (!(entity instanceof Api.Chat) || entity.id.toString() !== group.sourceChatId))
            throw new Error(`ETM group ${group.topicChatId} did not resolve to its expected basic group`)

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
            groupMessages.set(`${group.topicChatId}.${messageId}`, {
              topicChatId: group.topicChatId,
              sourceChatId: group.sourceChatId,
              messageId,
              senderId,
              timestamp: message.date,
              text: message.message,
              topicId,
            })
          }
          if (task.state.lastError)
            throw task.state.rawError ?? new Error(task.state.lastError)
        }
        catch (error) {
          const category = classifyUnavailableBoundGroup(error)
          if (!category)
            throw error
          const unavailableGroup = {
            topicChatId: group.topicChatId,
            sourceChatId: group.sourceChatId,
            category,
          } satisfies UnavailableBoundGroup
          const unavailableBindings = unavailableBindingReports(
            bindingsForGroup(before.bindings, group.topicChatId),
            inspectionSlaveNames(before),
          )
          const unavailableEvent = {
            type: 'group-unavailable',
            taskId,
            version: 2,
            ...unavailableGroup,
            bindingCount: unavailableBindings.length,
            bindings: unavailableBindings,
          } as const
          counts['unavailable-bound-group'] += 1
          logger.withFields(unavailableGroup).warn('Skipping unavailable ETM bound group')
          if (input.outputFile) {
            await appendReportEvent(input.outputFile, {
              runId: taskId,
              ...unavailableEvent,
              totalCounts: counts,
              totalExamined: examined,
            })
          }
          yield unavailableEvent
          yield { type: 'progress', taskId, topicChatId: group.topicChatId, sourceChatId: group.sourceChatId, examined }
          continue
        }
        finally {
          signal?.removeEventListener('abort', abortTask)
        }

        const afterGroup = await inspector(input.etm)
        const initialGroupBindings = bindingsForGroup(before.bindings, group.topicChatId)
        const currentGroupBindings = bindingsForGroup(afterGroup.bindings, group.topicChatId)
        if (!sameBindings(initialGroupBindings, currentGroupBindings))
          throw new Error(`ETM TopicAssoc mappings changed for ${group.topicChatId} during Telegram acquisition; repair aborted`)

        const bindings = new Map(currentGroupBindings.map(binding => [`${binding.topicChatId}.${binding.messageThreadId}`, binding]))
        const groupResult = buildGroupCandidates(groupMessages.values(), bindings, roles)
        const presences = await presenceReader(input.etm, groupResult.candidates.map(candidate => candidate.identity))
        const missing = splitCandidatesByPresence(groupResult.candidates, presences, groupResult.counts, groupResult.slaveCounts)
        const outcome = await inserter(input.etm, missing, input.chunkSize)
        applyInsertOutcome(groupResult.counts, outcome)
        applySlaveInsertOutcome(missing, outcome, groupResult.slaveCounts)
        addCounts(counts, groupResult.counts)
        examined += groupMessages.size
        const slaveSummaries = buildSlaveSummaryEvents(
          taskId,
          group.topicChatId,
          inspectionSlaveNames(afterGroup),
          groupResult.slaveCounts,
        )
        const groupComplete = {
          type: 'group-complete',
          taskId,
          version: 2,
          topicChatId: group.topicChatId,
          sourceChatId: group.sourceChatId,
          slaveCount: slaveSummaries.length,
          mappedExamined: slaveSummaries.reduce((total, event) => total + event.counts.mappedExamined, 0),
        } as const

        if (input.outputFile) {
          for (const event of slaveSummaries)
            await appendReportEvent(input.outputFile, { runId: taskId, ...event })
          await appendReportEvent(input.outputFile, {
            runId: taskId,
            ...groupComplete,
            counts: groupResult.counts,
            totalCounts: counts,
            examined: groupMessages.size,
            totalExamined: examined,
            candidates: groupResult.candidates.map(candidate => reportCandidate(candidate, presences)),
          })
        }

        for (const event of slaveSummaries)
          yield event
        yield groupComplete
        yield { type: 'progress', taskId, topicChatId: group.topicChatId, sourceChatId: group.sourceChatId, examined }
      }

      const summary = buildSummary(input, toMs, groups, roles, counts, examined)
      if (input.outputFile) {
        await appendReportEvent(input.outputFile, {
          type: 'run-complete',
          version: 2,
          runId: taskId,
          summary,
        })
      }
      yield { type: 'completed', summary, file: input.outputFile ? basename(input.outputFile) : null }
    }
    catch (error) {
      if (input.outputFile && reportStarted) {
        await appendReportEvent(input.outputFile, {
          type: 'run-failed',
          version: 2,
          runId: taskId,
          category: safeFailureCategory(error),
          totalCounts: counts,
          totalExamined: examined,
        }).catch(reportError => logger.withError(reportError).warn('Failed to append recovery failure report'))
      }
      throw error
    }
  }
}
