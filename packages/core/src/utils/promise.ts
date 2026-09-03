import type { CoreEmitter, CoreEvent, ExtractData } from '../context'

export class PromiseTimeoutError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'PromiseTimeoutError'
  }
}

export async function withTimeout<T>(operation: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new PromiseTimeoutError(message)), timeoutMs)
      }),
    ])
  }
  finally {
    if (timeout)
      clearTimeout(timeout)
  }
}

export function waitForEvent<E extends keyof CoreEvent>(
  emitter: CoreEmitter,
  event: E,
): Promise<ExtractData<CoreEvent[E]>> {
  return new Promise((resolve) => {
    // emitter.once(event, (data) => {
    // resolve(data)

    emitter.once(event, (...args) => {
      resolve(args[0] as ExtractData<CoreEvent[E]>)
    })
  })
}
