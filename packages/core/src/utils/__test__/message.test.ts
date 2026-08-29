import bigInt from 'big-integer'

import { Api } from 'telegram'
import { describe, expect, it } from 'vitest'

import { convertToCoreMessage } from '../message'

describe('convertToCoreMessage', () => {
  it('preserves the forum topic root independently from the direct reply target', () => {
    const message = {
      id: 42,
      date: 1_700_000_000,
      message: 'topic reply',
      peerId: new Api.PeerChannel({ channelId: bigInt(100) }),
      sender: new Api.User({ id: bigInt(200), firstName: 'Alice' }),
      senderId: bigInt(200),
      replyTo: new Api.MessageReplyHeader({
        replyToMsgId: 41,
        replyToTopId: 10,
      }),
    } as unknown as Api.Message

    const converted = convertToCoreMessage(message).unwrap()

    expect(converted.reply).toMatchObject({
      isReply: true,
      replyToId: '41',
      replyToTopId: '10',
    })
  })
})
