import { describe, it, expect, vi, beforeEach } from 'vitest';

// registerChannelAdapter runs at import time — mock it so importing the
// module under test doesn't require a live registry.
vi.mock('./channel-registry.js', () => ({ registerChannelAdapter: vi.fn() }));

import { GmailChannel } from './gmail.js';

describe('GmailChannel', () => {
  let channel: GmailChannel;

  beforeEach(() => {
    channel = new GmailChannel();
  });

  describe('identity', () => {
    it('declares the gmail channel type and native (non-threaded) shape', () => {
      expect(channel.name).toBe('gmail');
      expect(channel.channelType).toBe('gmail');
      expect(channel.supportsThreads).toBe(false);
    });

    it('declares DM-shaped defaults with no mention concept and no sender gating', () => {
      expect(channel.defaults).toEqual({
        dm: { engageMode: 'pattern', engagePattern: '.', threads: false, unknownSenderPolicy: 'public' },
        group: { engageMode: 'pattern', engagePattern: '.', threads: false, unknownSenderPolicy: 'public' },
        mentions: 'never',
      });
    });
  });

  describe('isConnected', () => {
    it('returns false before setup', () => {
      expect(channel.isConnected()).toBe(false);
    });
  });

  describe('teardown', () => {
    it('leaves the channel disconnected', async () => {
      await channel.teardown();
      expect(channel.isConnected()).toBe(false);
    });
  });

  describe('deliver', () => {
    it('no-ops when not connected', async () => {
      const result = await channel.deliver('thread-1', null, { kind: 'chat', content: { text: 'hi' } });
      expect(result).toBeUndefined();
    });
  });

  describe('constructor options', () => {
    it('accepts a custom poll interval', () => {
      const ch = new GmailChannel(30000);
      expect(ch.name).toBe('gmail');
    });

    it('defaults to unread-primary query, excluding the newsletter label', () => {
      const ch = new GmailChannel();
      const query = (ch as unknown as { buildQuery: () => string }).buildQuery();
      expect(query).toBe('is:unread category:primary -label:newsletter');
    });
  });
});
