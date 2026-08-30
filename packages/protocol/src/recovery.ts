import type { InferOutput } from 'valibot'

import type { AppError } from './errors'

import { defineInvokeEventa } from '@moeru/eventa'
import { array, boolean, literal, minLength, nullable, number, object, optional, pipe, string, union } from 'valibot'

export const recoveryRepairInputSchema = object({
  etm: union([
    object({ backend: literal('sqlite'), path: pipe(string(), minLength(1)) }),
    object({ backend: literal('postgres'), url: pipe(string(), minLength(1)) }),
  ]),
  fromMs: number(),
  toMs: number(),
  mainBotUsername: pipe(string(), minLength(1)),
  auxiliaryBotUsernames: array(pipe(string(), minLength(1))),
  chunkSize: number(),
  outputFile: optional(nullable(pipe(string(), minLength(1)))),
  takeout: boolean(),
})

export type RecoveryRepairInput = InferOutput<typeof recoveryRepairInputSchema>

export interface RecoveryRepairCounts {
  'present-primary': number
  'present-alt': number
  'inserted': number
  'unbound-topic': number
  'human-or-unverified-sender': number
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
  mainBotId: string
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
