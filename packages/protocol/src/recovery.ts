import type { InferOutput } from 'valibot'

import type { AppError } from './errors'

import { defineInvokeEventa } from '@moeru/eventa'
import { array, boolean, literal, minLength, nullable, number, object, optional, pipe, string, union } from 'valibot'

export const RECOVERY_REPAIR_FROM_ISO = '2026-07-13T18:22:03Z'

export const recoveryRepairInputSchema = object({
  etm: union([
    object({ backend: literal('sqlite'), path: pipe(string(), minLength(1)) }),
    object({
      backend: literal('postgres'),
      database: pipe(string(), minLength(1)),
      host: pipe(string(), minLength(1)),
      port: number(),
      user: pipe(string(), minLength(1)),
      password: string(),
      maxConnections: number(),
      staleTimeout: number(),
      options: string(),
    }),
  ]),
  mainBotId: pipe(string(), minLength(1)),
  auxiliaryBotIds: array(pipe(string(), minLength(1))),
  startedAtMs: number(),
  chunkSize: number(),
  outputFile: optional(nullable(pipe(string(), minLength(1)))),
  takeout: boolean(),
})

export type RecoveryRepairInput = InferOutput<typeof recoveryRepairInputSchema>

export interface RecoveryRepairCounts {
  'present-primary': number
  'present-alt': number
  'inserted': number
  'unavailable-bound-group': number
  'unbound-topic': number
  'human-or-unconfigured-sender': number
  'service-deleted-unusable': number
  'concurrent': number
  'conflicts': number
  'errors': number
}

export interface RecoveryRepairSlaveCounts {
  mappedExamined: number
  eligible: number
  presentPrimary: number
  presentAlt: number
  inserted: number
  concurrent: number
  conflicts: number
  errors: number
  skipped: {
    'human-or-unconfigured-sender': number
    'service-deleted-unusable': number
  }
}

export type RecoveryRepairNameSource = 'slavechatinfo.slave_chat_name' | 'msglog.slave_origin_display_name' | 'slave_uid'

export interface RecoveryRepairSummary {
  version: 1
  backend: 'sqlite' | 'postgres'
  window: { from: string, to: string, semantics: '[from,to)' }
  groups: string[]
  mainBotIds: string[]
  auxiliaryBotIds: string[]
  counts: RecoveryRepairCounts
  examined: number
}

export type RecoveryRepairUpdate
  = | { type: 'started', taskId: string }
    | {
      type: 'recovery-stage'
      taskId: string
      version: 2
      stage: 'etm-inspection'
      status: 'started' | 'completed'
      bindingCount?: number
    }
    | {
      type: 'recovery-stage'
      taskId: string
      version: 2
      stage: 'historical-group-discovery'
      status: 'started' | 'completed'
      historicalGroupCount?: number
      groupCount?: number
    }
    | {
      type: 'group-start'
      taskId: string
      version: 2
      topicChatId: string
      sourceChatId: string
      source: 'topic-assoc' | 'msglog-history'
      bindingCount: number
    }
    | {
      type: 'group-acquisition-progress'
      taskId: string
      version: 2
      topicChatId: string
      sourceChatId: string
      acquired: number
    }
    | {
      type: 'group-acquisition-heartbeat'
      taskId: string
      version: 2
      topicChatId: string
      sourceChatId: string
      acquired: number
      elapsedMs: number
      idleMs: number
    }
    | { type: 'progress', taskId: string, topicChatId: string, sourceChatId: string, examined: number }
    | {
      type: 'topic-binding-discovery'
      taskId: string
      version: 2
      topicChatId: string
      messageThreadId: string
      status: 'started'
    }
    | {
      type: 'topic-binding-discovery'
      taskId: string
      version: 2
      topicChatId: string
      messageThreadId: string
      status: 'completed'
      anchorsExamined: number
      outcome: 'resolved' | 'conflict' | 'not-found'
      slaveUid?: string
      slaveUids?: string[]
    }
    | {
      type: 'topic-binding-discovery-heartbeat'
      taskId: string
      version: 2
      topicChatId: string
      messageThreadId: string
      anchorsChecked: number
      elapsedMs: number
      idleMs: number
    }
    | {
      type: 'topic-binding'
      taskId: string
      version: 2
      topicChatId: string
      messageThreadId: string
      slaveUid: string
      source: 'topic-assoc' | 'msglog-history'
    }
    | {
      type: 'topic-binding-conflict'
      taskId: string
      version: 2
      topicChatId: string
      messageThreadId: string
      slaveUids: string[]
      source: 'msglog-history'
    }
    | {
      type: 'slave-summary'
      taskId: string
      version: 2
      topicChatId: string
      slaveUid: string
      slaveName: string
      nameSource: RecoveryRepairNameSource
      counts: RecoveryRepairSlaveCounts
    }
    | {
      type: 'group-complete'
      taskId: string
      version: 2
      topicChatId: string
      sourceChatId: string
      slaveCount: number
      mappedExamined: number
    }
    | {
      type: 'group-unavailable'
      taskId: string
      version: 2
      topicChatId: string
      sourceChatId: string
      category:
        | 'broadcast-channel'
        | 'channel-invalid'
        | 'channel-private'
        | 'channel-public-group-na'
        | 'chat-id-invalid'
        | 'peer-id-invalid'
        | 'user-not-participant'
      bindingCount: number
      bindings: Array<{
        messageThreadId: string
        slaveUid: string
        slaveName: string
        nameSource: RecoveryRepairNameSource
      }>
    }
    | { type: 'completed', summary: RecoveryRepairSummary, file: string | null }
    | { type: 'failed', taskId: string, error: AppError }

export const recoveryContracts = {
  repair: defineInvokeEventa<RecoveryRepairUpdate, RecoveryRepairInput>('tg.v1.recovery.repair'),
}
