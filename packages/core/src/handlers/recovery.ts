import type { EventContext } from '@moeru/eventa'

import type { TelegramApplication } from '../application/runtime'

import { defineStreamInvokeHandler } from '@moeru/eventa'
import { recoveryContracts, recoveryExportInputSchema } from '@tg-search/protocol'
import { v4 as uuidv4 } from 'uuid'
import { safeParse } from 'valibot'

import { invalidArgument } from '../application/errors'

type RecoveryApplication = TelegramApplication & {
  exportRecovery: NonNullable<TelegramApplication['exportRecovery']>
}

export function registerRecoveryHandler(context: EventContext<any, any>, application: RecoveryApplication) {
  defineStreamInvokeHandler(context, recoveryContracts.export, async function* (input, options) {
    const parsed = safeParse(recoveryExportInputSchema, input)
    if (!parsed.success || parsed.output.fromMs >= parsed.output.toMs) {
      yield { type: 'failed', taskId: uuidv4(), error: invalidArgument('Recovery export requires a valid [from,to) range').error }
      return
    }
    if (!parsed.output.takeout) {
      yield {
        type: 'failed',
        taskId: uuidv4(),
        error: {
          code: 'TAKEOUT_CONSENT_REQUIRED',
          message: 'Recovery export requires explicit Telegram Takeout consent',
          retryable: false,
        },
      }
      return
    }
    yield* application.exportRecovery(parsed.output, options?.abortController?.signal)
  })
}
