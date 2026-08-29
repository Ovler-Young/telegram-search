import type { Logger } from '@guiiai/logg'

import type { CoreContext } from '../../context'
import type { EntityService } from '../entity'
import type { TakeoutService } from '../takeout'

import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import bigInt from 'big-integer'

import { EventEmitter } from 'eventemitter3'
import { Api } from 'telegram'
import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  createRecoveryExportService,
  parseTopicChatId,
  resolveRecoveryChats,
} from '../recovery-export'

const temporaryDirectories: string[] = []

function logger(): Logger {
  const value: Record<string, unknown> = {}
  const chain = () => value
  for (const method of ['withContext', 'withFields', 'withError', 'withLogLevel', 'withLogLevelString', 'useGlobalConfig'])
    value[method] = chain
  for (const method of ['debug', 'verbose', 'log', 'warn', 'error'])
    value[method] = () => {}
  return value as unknown as Logger
}

function supergroup(id: string, title = 'Recovery group') {
  return new Api.Channel({
    id: bigInt(id),
    accessHash: bigInt(99),
    title,
    photo: new Api.ChatPhotoEmpty(),
    date: 0,
    megagroup: true,
  })
}

function basicGroup(id: string) {
  return new Api.Chat({
    id: bigInt(id),
    title: 'Basic group',
    photo: new Api.ChatPhotoEmpty(),
    participantsCount: 2,
    date: 0,
    version: 1,
    creator: false,
    left: false,
    deactivated: false,
    callActive: false,
    callNotEmpty: false,
    noforwards: false,
  })
}

afterEach(async () => {
  vi.restoreAllMocks()
  await Promise.all(temporaryDirectories.splice(0).map(path => rm(path, { recursive: true, force: true })))
})

describe('recovery chat selection', () => {
  it('normalizes marked supergroup and basic-group IDs without numeric coercion', () => {
    expect(parseTopicChatId('-10012345678901234567890')).toEqual({
      topicChatId: '-10012345678901234567890',
      sourceChatId: '10012345677901234567890',
      expectedPeer: 'channel',
    })
    expect(parseTopicChatId('-9007199254740993')).toEqual({
      topicChatId: '-9007199254740993',
      sourceChatId: '9006199254740993',
      expectedPeer: 'channel',
    })
    expect(parseTopicChatId('-42')).toEqual({ topicChatId: '-42', sourceChatId: '42', expectedPeer: 'chat' })
    expect(() => parseTopicChatId('42.0')).toThrow('Invalid Telegram chat ID')
  })

  it('deduplicates in first-seen order and rejects peer mismatches and canonical collisions', async () => {
    const resolved = await resolveRecoveryChats(['-42', '-42', '-100123'], async id => basicGroup(id === '-42' ? '42' : '100123'))
    expect(resolved.map(chat => chat.topicChatId)).toEqual(['-42', '-100123'])

    await expect(resolveRecoveryChats(['-1000000000042'], async () => supergroup('43')))
      .rejects
      .toThrow('did not resolve')

    await expect(resolveRecoveryChats(['-1000000000042', '42'], async () => supergroup('42')))
      .rejects
      .toThrow('resolve to the same source chat')

    await expect(resolveRecoveryChats(['-42'], async () => supergroup('42')))
      .rejects
      .toThrow('not a basic group')

    await expect(resolveRecoveryChats(['-42'], async () => {
      throw new Error('unknown peer')
    })).rejects.toThrow('unknown peer')
  })
})

describe('recovery artifact', () => {
  it('acquires each deduplicated selected group exactly once in file order', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'tg-recovery-'))
    temporaryDirectories.push(directory)
    const selected: string[] = []
    const entities = new Map([
      ['42', basicGroup('42')],
      ['43', basicGroup('43')],
    ])
    const context = {
      emitter: new EventEmitter(),
      getClient: () => ({
        getEntity: vi.fn(async (peer: Api.InputPeerChat) => entities.get(peer.chatId.toString())!),
        getMe: vi.fn(async () => ({ id: bigInt('9007199254740993123'), firstName: 'Owner' })),
      }),
      getMyUser: () => ({ id: '777', username: 'owner', name: 'Owner Account' }),
    } as unknown as CoreContext
    const service = createRecoveryExportService({
      context,
      logger: logger(),
      entityService: {
        getInputPeer: vi.fn(async id => new Api.InputPeerChat({ chatId: bigInt(String(id).replace(/^-/, '')) })),
      },
      takeoutService: {
        async* takeoutMessages(chatId) {
          selected.push(chatId)
          yield* []
        },
      },
    })

    for await (const _update of service({
      profile: 'owner-profile',
      topicChatIds: ['-43', '-42', '-43'],
      fromMs: 100_000,
      toMs: 200_000,
      outputFile: join(directory, 'recovery.jsonl'),
      takeout: true,
    })) {
      void _update
    }

    expect(selected).toEqual(['43', '42'])
  })

  it('exports only selected [from,to) messages with owner, mapping, and topic reply fields', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'tg-recovery-'))
    temporaryDirectories.push(directory)
    const outputFile = join(directory, 'recovery.jsonl')
    const sourceChatId = '42'
    const topicChatId = '-1000000000042'
    const channel = supergroup(sourceChatId)
    const inputPeer = new Api.InputPeerChannel({ channelId: bigInt(sourceChatId), accessHash: bigInt(99) })
    const messages = [99, 100, 150, 200].map(date => new Api.Message({
      id: date,
      peerId: new Api.PeerChannel({ channelId: bigInt(sourceChatId) }),
      fromId: new Api.PeerUser({ userId: bigInt('9007199254740993123') }),
      date,
      message: `at-${date}`,
      replyTo: date === 150
        ? new Api.MessageReplyHeader({ replyToMsgId: 149, replyToTopId: 120 })
        : undefined,
    }))
    const takeoutMessages = vi.fn(async function* (
      chatId: string,
      options: Parameters<TakeoutService['takeoutMessages']>[1],
    ) {
      expect(chatId).toBe(sourceChatId)
      expect(options.inputPeer).toBe(inputPeer)
      expect(options.startTime).toBe(100_000)
      expect(options.endTime).toBe(199_999)
      expect(options.expectedCount).toBe(0)
      yield* messages
    })
    const context = {
      emitter: new EventEmitter(),
      getClient: () => ({
        getEntity: vi.fn(async () => channel),
        getMe: vi.fn(async () => ({
          id: bigInt('9007199254740993123'),
          firstName: 'Owner',
          lastName: 'Account',
          username: undefined,
        })),
      }),
      getMyUser: () => ({ id: '777', username: 'owner', name: 'Owner Account' }),
    } as unknown as CoreContext
    const entityService = {
      getInputPeer: vi.fn(async () => inputPeer),
    } as Pick<EntityService, 'getInputPeer'>
    const service = createRecoveryExportService({
      context,
      logger: logger(),
      entityService,
      takeoutService: { takeoutMessages },
    })

    const updates = []
    for await (const update of service({
      profile: 'owner-profile',
      topicChatIds: [topicChatId],
      fromMs: 100_000,
      toMs: 200_000,
      outputFile,
      takeout: true,
    })) {
      updates.push(update)
    }

    expect(updates.at(-1)).toMatchObject({ type: 'completed', exported: 2 })
    const records = (await readFile(outputFile, 'utf8')).trim().split('\n').map(line => JSON.parse(line))
    expect(records[0]).toMatchObject({
      type: 'manifest',
      version: 1,
      owner: {
        profile: 'owner-profile',
        telegramUserId: '9007199254740993123',
        username: null,
        name: 'Owner Account',
      },
      window: { semantics: '[from,to)' },
      chats: [{ topicChatId, sourceChatId }],
    })
    expect(records.slice(1)).toEqual([
      expect.objectContaining({ messageId: '100', senderId: '9007199254740993123', replyToTopId: null }),
      expect.objectContaining({ messageId: '150', replyToId: '149', replyToTopId: '120' }),
    ])
    expect(takeoutMessages).toHaveBeenCalledTimes(1)
  })
})
