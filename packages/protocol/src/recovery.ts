import type { InferOutput } from 'valibot'

import type { AppError } from './errors'

import { defineInvokeEventa } from '@moeru/eventa'
import { boolean, literal, minLength, number, object, pipe, string, union } from 'valibot'

export const recoveryAuditInputSchema = object({
  etm: union([
    object({ backend: literal('sqlite'), path: pipe(string(), minLength(1)) }),
    object({ backend: literal('postgres'), url: pipe(string(), minLength(1)) }),
  ]),
  fromMs: number(),
  toMs: number(),
  outputFile: pipe(string(), minLength(1)),
  takeout: boolean(),
})

export type RecoveryAuditInput = InferOutput<typeof recoveryAuditInputSchema>

export type RecoveryAuditUpdate
  = | { type: 'started', taskId: string }
    | { type: 'progress', taskId: string, topicChatId: string, sourceChatId: string, audited: number }
    | { type: 'completed', taskId: string, file: string, audited: number }
    | { type: 'failed', taskId: string, error: AppError }

export const recoveryContracts = {
  audit: defineInvokeEventa<RecoveryAuditUpdate, RecoveryAuditInput>('tg.v1.recovery.audit'),
}
