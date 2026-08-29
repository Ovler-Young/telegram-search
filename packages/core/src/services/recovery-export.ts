import type { Logger } from '@guiiai/logg'
import type { RecoveryExportInput, RecoveryExportUpdate } from '@tg-search/protocol'

import type { CoreContext } from '../context'
import type { EntityService } from './entity'
import type { TakeoutService } from './takeout'

import { mkdir, rename, rm, writeFile } from 'node:fs/promises'
import { basename, dirname } from 'node:path'

import { Api } from 'telegram'
import { v4 as uuidv4 } from 'uuid'

import { parseMediaType } from '../utils/media'
import { createTask } from '../utils/task'

const BOT_API_CHANNEL_MARK = 1_000_000_000_000n

export interface ParsedTopicChatId {
  topicChatId: string
  sourceChatId: string
  expectedPeer: 'channel' | 'chat' | 'group'
}

export interface RecoveryChatMapping extends ParsedTopicChatId {
  title: string
  type: 'supergroup' | 'group'
}

export function parseTopicChatId(value: string): ParsedTopicChatId {
  if (!/^-?\d+$/.test(value))
    throw new Error(`Invalid Telegram chat ID: ${value}`)

  const parsed = BigInt(value)
  if (parsed === 0n)
    throw new Error('Telegram chat ID must not be zero')

  const topicChatId = parsed.toString()
  if (parsed <= -(BOT_API_CHANNEL_MARK + 1n)) {
    return {
      topicChatId,
      sourceChatId: (-parsed - BOT_API_CHANNEL_MARK).toString(),
      expectedPeer: 'channel',
    }
  }
  if (parsed < 0n) {
    return {
      topicChatId,
      sourceChatId: (-parsed).toString(),
      expectedPeer: 'chat',
    }
  }
  return { topicChatId, sourceChatId: topicChatId, expectedPeer: 'group' }
}

function entityId(entity: Api.TypeUser | Api.TypeChat): string | undefined {
  if (entity instanceof Api.Chat || entity instanceof Api.Channel)
    return entity.id.toString()
  return undefined
}

export async function resolveRecoveryChats(
  topicChatIds: string[],
  resolve: (topicChatId: string) => Promise<Api.TypeUser | Api.TypeChat>,
): Promise<RecoveryChatMapping[]> {
  const unique = new Map<string, ParsedTopicChatId>()
  for (const value of topicChatIds) {
    const parsed = parseTopicChatId(value)
    if (!unique.has(parsed.topicChatId))
      unique.set(parsed.topicChatId, parsed)
  }

  const sourceOwners = new Map<string, string>()
  const mappings: RecoveryChatMapping[] = []
  for (const parsed of unique.values()) {
    const owner = sourceOwners.get(parsed.sourceChatId)
    if (owner && owner !== parsed.topicChatId) {
      throw new Error(`Chat IDs ${owner} and ${parsed.topicChatId} resolve to the same source chat ${parsed.sourceChatId}`)
    }

    const entity = await resolve(parsed.topicChatId)
    const resolvedId = entityId(entity)
    if (!resolvedId || resolvedId !== parsed.sourceChatId)
      throw new Error(`Chat ID ${parsed.topicChatId} did not resolve to source chat ${parsed.sourceChatId}`)

    if (parsed.expectedPeer === 'channel' && (!(entity instanceof Api.Channel) || !entity.megagroup))
      throw new Error(`Chat ID ${parsed.topicChatId} is not a supergroup`)
    if (parsed.expectedPeer === 'chat' && !(entity instanceof Api.Chat))
      throw new Error(`Chat ID ${parsed.topicChatId} is not a basic group`)
    if (!(entity instanceof Api.Chat) && (!(entity instanceof Api.Channel) || !entity.megagroup))
      throw new Error(`Chat ID ${parsed.topicChatId} is not a group`)

    sourceOwners.set(parsed.sourceChatId, parsed.topicChatId)
    mappings.push({
      ...parsed,
      title: entity.title,
      type: entity instanceof Api.Channel ? 'supergroup' : 'group',
    })
  }
  return mappings
}

function rawPeerId(peer: Api.TypePeer): string | undefined {
  if (peer instanceof Api.PeerUser)
    return peer.userId.toString()
  if (peer instanceof Api.PeerChat)
    return peer.chatId.toString()
  if (peer instanceof Api.PeerChannel)
    return peer.channelId.toString()
  return undefined
}

function mediaRecord(media: Api.TypeMessageMedia | undefined): { type: string, mimeType: string | null } | null {
  if (!media)
    return null
  const mimeType = media instanceof Api.MessageMediaDocument && media.document instanceof Api.Document
    ? media.document.mimeType
    : media instanceof Api.MessageMediaPhoto
      ? 'image/jpeg'
      : null
  return { type: parseMediaType(media), mimeType }
}

function compareMessageRows(a: { timestamp: number, messageId: string }, b: { timestamp: number, messageId: string }): number {
  if (a.timestamp !== b.timestamp)
    return a.timestamp - b.timestamp
  const aId = BigInt(a.messageId)
  const bId = BigInt(b.messageId)
  return aId < bId ? -1 : aId > bId ? 1 : 0
}

export function createRecoveryExportService(options: {
  context: CoreContext
  logger: Logger
  entityService: Pick<EntityService, 'getInputPeer'>
  takeoutService: Pick<TakeoutService, 'takeoutMessages'>
}) {
  const { context, entityService, logger, takeoutService } = options

  return async function* exportRecovery(input: RecoveryExportInput, signal?: AbortSignal): AsyncGenerator<RecoveryExportUpdate> {
    const taskId = uuidv4()
    yield { type: 'started', taskId }

    const inputPeers = new Map<string, Api.TypeInputPeer>()
    const mappings = await resolveRecoveryChats(input.topicChatIds, async (topicChatId) => {
      const peer = await entityService.getInputPeer(topicChatId)
      inputPeers.set(topicChatId, peer)
      return context.getClient().getEntity(peer)
    })
    const owner = context.getMyUser()
    const temporaryPath = `${input.outputFile}.${taskId}.tmp`
    let exported = 0

    try {
      await mkdir(dirname(input.outputFile), { recursive: true })
      const manifest = {
        type: 'manifest',
        version: 1,
        owner: {
          profile: input.profile,
          telegramUserId: owner.id,
          username: owner.username,
          name: owner.name,
        },
        window: {
          from: new Date(input.fromMs).toISOString(),
          to: new Date(input.toMs).toISOString(),
          semantics: '[from,to)',
        },
        chats: mappings.map(({ topicChatId, sourceChatId, title, type }) => ({ topicChatId, sourceChatId, title, type })),
      }
      const lines = [`${JSON.stringify(manifest)}\n`]

      for (const mapping of mappings) {
        if (signal?.aborted)
          throw signal.reason instanceof Error ? signal.reason : new DOMException('Recovery export aborted', 'AbortError')

        const task = createTask('takeout', { chatIds: [mapping.sourceChatId] }, context.emitter, logger)
        const abortTask = () => task.abort()
        signal?.addEventListener('abort', abortTask, { once: true })
        const byMessageId = new Map<string, {
          type: 'message'
          version: 1
          topicChatId: string
          sourceChatId: string
          messageId: string
          senderId: string
          timestamp: number
          text: string
          replyToId: string | null
          replyToTopId: string | null
          media: { type: string, mimeType: string | null } | null
        }>()
        try {
          for await (const message of takeoutService.takeoutMessages(mapping.sourceChatId, {
            pagination: { limit: 100, offset: 0 },
            inputPeer: inputPeers.get(mapping.topicChatId),
            startTime: input.fromMs,
            // The generic Takeout layer has an inclusive end contract.
            endTime: input.toMs - 1,
            skipMedia: true,
            expectedCount: 0,
            disableAutoProgress: true,
            takeoutConsent: input.takeout,
            task,
          })) {
            const sourceChatId = rawPeerId(message.peerId)
            if (sourceChatId !== mapping.sourceChatId)
              throw new Error(`Telegram returned message ${message.id} from unselected chat ${sourceChatId ?? 'unknown'}`)

            const timestampMs = message.date * 1000
            if (timestampMs < input.fromMs || timestampMs >= input.toMs)
              continue

            const senderId = message.fromId ? rawPeerId(message.fromId) : undefined
            if (!senderId)
              throw new Error(`Message ${message.id} in chat ${mapping.sourceChatId} has no sender ID`)

            const messageId = message.id.toString()
            byMessageId.set(messageId, {
              type: 'message',
              version: 1,
              topicChatId: mapping.topicChatId,
              sourceChatId: mapping.sourceChatId,
              messageId,
              senderId,
              timestamp: message.date,
              text: message.message,
              replyToId: message.replyTo?.replyToMsgId?.toString() ?? null,
              replyToTopId: message.replyTo instanceof Api.MessageReplyHeader
                ? message.replyTo.replyToTopId?.toString() ?? null
                : null,
              media: mediaRecord(message.media),
            })
          }
        }
        finally {
          signal?.removeEventListener('abort', abortTask)
        }

        if (task.state.lastError)
          throw task.state.rawError ?? new Error(task.state.lastError)

        const rows = [...byMessageId.values()].sort(compareMessageRows)
        for (const row of rows)
          lines.push(`${JSON.stringify(row)}\n`)
        exported += rows.length
        yield { type: 'progress', taskId, topicChatId: mapping.topicChatId, sourceChatId: mapping.sourceChatId, exported }
      }

      await writeFile(temporaryPath, lines.join(''), { mode: 0o600 })
      if (signal?.aborted)
        throw signal.reason instanceof Error ? signal.reason : new DOMException('Recovery export aborted', 'AbortError')
      await rename(temporaryPath, input.outputFile)
    }
    catch (error) {
      await rm(temporaryPath, { force: true })
      throw error
    }

    yield { type: 'completed', taskId, file: basename(input.outputFile), exported }
  }
}
