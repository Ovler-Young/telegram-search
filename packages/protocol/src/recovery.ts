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
    | { type: 'progress', taskId: string, topicChatId: string, sourceChatId: string, examined: number }
    | { type: 'completed', summary: RecoveryRepairSummary, file: string | null }
    | { type: 'failed', taskId: string, error: AppError }

export const recoveryContracts = {
  repair: defineInvokeEventa<RecoveryRepairUpdate, RecoveryRepairInput>('tg.v1.recovery.repair'),
}
