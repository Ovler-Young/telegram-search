import type { InferOutput } from 'valibot'

import type { AppError } from './errors'

import { defineInvokeEventa } from '@moeru/eventa'
import { array, boolean, minLength, number, object, pipe, string } from 'valibot'

export const recoveryExportInputSchema = object({
  profile: pipe(string(), minLength(1)),
  topicChatIds: pipe(array(pipe(string(), minLength(1))), minLength(1)),
  fromMs: number(),
  toMs: number(),
  outputFile: pipe(string(), minLength(1)),
  takeout: boolean(),
})

export type RecoveryExportInput = InferOutput<typeof recoveryExportInputSchema>

export type RecoveryExportUpdate
  = | { type: 'started', taskId: string }
    | { type: 'progress', taskId: string, topicChatId: string, sourceChatId: string, exported: number }
    | { type: 'completed', taskId: string, file: string, exported: number }
    | { type: 'failed', taskId: string, error: AppError }

export const recoveryContracts = {
  export: defineInvokeEventa<RecoveryExportUpdate, RecoveryExportInput>('tg.v1.recovery.export'),
}
