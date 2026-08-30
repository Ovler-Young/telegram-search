import type { EventContext } from '@moeru/eventa'

import type { TelegramApplication } from '../application/runtime'

import { defineStreamInvokeHandler } from '@moeru/eventa'
import { recoveryContracts, recoveryRepairInputSchema } from '@tg-search/protocol'
import { v4 as uuidv4 } from 'uuid'
import { safeParse } from 'valibot'

import { invalidArgument } from '../application/errors'

type RecoveryApplication = TelegramApplication & {
  repairRecovery: NonNullable<TelegramApplication['repairRecovery']>
}

export function registerRecoveryHandler(context: EventContext<any, any>, application: RecoveryApplication) {
  defineStreamInvokeHandler(context, recoveryContracts.repair, async function* (input, options) {
    const parsed = safeParse(recoveryRepairInputSchema, input)
    if (!parsed.success || parsed.output.fromMs >= parsed.output.toMs || !Number.isInteger(parsed.output.chunkSize) || parsed.output.chunkSize <= 0) {
      yield { type: 'failed', taskId: uuidv4(), error: invalidArgument('Recovery repair requires a valid [from,to) range and positive integer chunk size').error }
      return
    }
    if (!parsed.output.takeout) {
      yield {
        type: 'failed',
        taskId: uuidv4(),
        error: {
          code: 'TAKEOUT_CONSENT_REQUIRED',
          message: 'Recovery repair requires explicit Telegram Takeout consent',
          retryable: false,
        },
      }
      return
    }
    yield* application.repairRecovery(parsed.output, options?.abortController?.signal)
  })
}
