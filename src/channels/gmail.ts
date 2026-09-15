/**
 * Gmail channel — polls the Gmail inbox for new primary/unread mail and
 * routes it through the normal v2 inbound path; the agent's responses go
 * back out via the Gmail API as notes-to-self, threaded into the original
 * conversation with In-Reply-To/References.
 *
 * SAFETY: every deliver() call is addressed to the mailbox owner
 * (this.userEmail), never to the original external sender. No tool has
 * ever existed for the agent to deliberately reply to a third party — so
 * without this, EVERY ordinary agent turn (even a plain "got it, here's a
 * summary") becomes a real outgoing email to whoever last emailed in. That
 * caused a real incident (2026-09-15): hours of auto-generated replies
 * landing on newsletter senders, a talent-pool system, a lottery marketing
 * address, mailer-daemon bounce notices (a self-sustaining bounce loop),
 * and a live Hetzner support ticket. Do not change `To:` in deliver() to
 * anything other than this.userEmail without a deliberate, explicit,
 * agent-invoked "reply to sender" action gating it.
 *
 * Native adapter, host-side only (like cli.ts) — googleapis calls happen
 * in this process, not inside the agent container. OAuth credentials live
 * on the host at ~/.gmail-mcp/{gcp-oauth.keys.json,credentials.json}, set
 * up via /add-gmail. Not mounted into agent containers: ~/.gmail-mcp sits
 * outside the project tree, so it doesn't qualify for the 'install-surface'
 * mount class (reserved for paths under the release's own surface roots —
 * container/agent-runner/src, container/skills, a group's stamped plugins/
 * dir). Giving the agent its own Gmail tools would need a properly vetted
 * 'allowlisted-extra' mount (the mount-allowlist feature, admin-approved)
 * or an MCP server fed credentials through OneCLI instead of a raw file.
 *
 * One shared platformId for the whole mailbox (the account's own address) —
 * every email becomes a message in a single ongoing conversation, tagged
 * with its sender/subject, same as the original NanoClaw v1 design. This is
 * a deliberate choice over "one platformId per Gmail thread": mentions are
 * declared 'never', so there is no auto-wire-on-first-contact flow here —
 * the operator wires this one conversation once via `ncl`, and every email
 * flows through it rather than spawning a fresh wiring prompt per thread
 * (which would be unworkable for a normal inbox). Replies target whichever
 * thread was most recently delivered (`lastThreadId`) — correct for the
 * common case of discussing the latest email, same ambiguity a human
 * assistant would have if you said "reply to that" after several emails.
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

import { google, gmail_v1 } from 'googleapis';
import { OAuth2Client } from 'google-auth-library';

import { log } from '../log.js';
import { registerChannelAdapter } from './channel-registry.js';
import type { ChannelAdapter, ChannelDefaults, ChannelSetup, OutboundMessage } from './adapter.js';

interface ThreadMeta {
  sender: string;
  senderName: string;
  subject: string;
  messageId: string; // RFC 2822 Message-ID, for In-Reply-To/References
}

/**
 * Email is DM-shaped: every inbound message engages the wired agent
 * (pattern '.'), there's no group/thread concept, and no mention signal —
 * this single mailbox-wide conversation is wired once by the operator
 * (`ncl`), not auto-created per first contact. `unknownSenderPolicy` is
 * 'public': the "sender" here is an arbitrary external email address, not
 * a NanoClaw platform identity worth gating — every email is meant to
 * reach the agent as a notification, same trust model as the CLI channel's
 * owner-only socket.
 */
const GMAIL_DEFAULTS: ChannelDefaults = {
  dm: { engageMode: 'pattern', engagePattern: '.', threads: false, unknownSenderPolicy: 'public' },
  group: { engageMode: 'pattern', engagePattern: '.', threads: false, unknownSenderPolicy: 'public' },
  mentions: 'never',
};

function credPaths(): { keysPath: string; tokensPath: string } {
  const credDir = path.join(os.homedir(), '.gmail-mcp');
  return {
    keysPath: path.join(credDir, 'gcp-oauth.keys.json'),
    tokensPath: path.join(credDir, 'credentials.json'),
  };
}

function hasCredentials(): boolean {
  const { keysPath, tokensPath } = credPaths();
  return fs.existsSync(keysPath) && fs.existsSync(tokensPath);
}

export class GmailChannel implements ChannelAdapter {
  name = 'gmail';
  channelType = 'gmail';
  supportsThreads = false;
  defaults = GMAIL_DEFAULTS;

  private oauth2Client: OAuth2Client | null = null;
  private gmail: gmail_v1.Gmail | null = null;
  private config: ChannelSetup | null = null;
  private pollIntervalMs: number;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private processedIds = new Set<string>();
  private threadMeta = new Map<string, ThreadMeta>();
  private lastThreadId: string | null = null;
  private consecutiveErrors = 0;
  private userEmail = '';

  constructor(pollIntervalMs = 60000) {
    this.pollIntervalMs = pollIntervalMs;
  }

  async setup(config: ChannelSetup): Promise<void> {
    this.config = config;

    const { keysPath, tokensPath } = credPaths();
    if (!hasCredentials()) {
      log.warn('Gmail credentials not found in ~/.gmail-mcp/. Skipping Gmail channel. Run /add-gmail to set up.');
      return;
    }

    const keys = JSON.parse(fs.readFileSync(keysPath, 'utf-8'));
    const tokens = JSON.parse(fs.readFileSync(tokensPath, 'utf-8'));

    const clientConfig = keys.installed || keys.web || keys;
    const { client_id, client_secret, redirect_uris } = clientConfig;
    this.oauth2Client = new google.auth.OAuth2(client_id, client_secret, redirect_uris?.[0]);
    this.oauth2Client.setCredentials(tokens);

    // Persist refreshed tokens back to the host credential store.
    this.oauth2Client.on('tokens', (newTokens) => {
      try {
        const current = JSON.parse(fs.readFileSync(tokensPath, 'utf-8'));
        Object.assign(current, newTokens);
        fs.writeFileSync(tokensPath, JSON.stringify(current, null, 2));
        log.debug('Gmail OAuth tokens refreshed');
      } catch (err) {
        log.warn('Failed to persist refreshed Gmail tokens', { err });
      }
    });

    this.gmail = google.gmail({ version: 'v1', auth: this.oauth2Client });

    const profile = await this.gmail.users.getProfile({ userId: 'me' });
    this.userEmail = profile.data.emailAddress || '';
    log.info('Gmail channel connected', { email: this.userEmail });
    this.config.onMetadata(this.userEmail, `Gmail (${this.userEmail})`, false);

    const schedulePoll = () => {
      const backoffMs =
        this.consecutiveErrors > 0
          ? Math.min(this.pollIntervalMs * Math.pow(2, this.consecutiveErrors), 30 * 60 * 1000)
          : this.pollIntervalMs;
      this.pollTimer = setTimeout(() => {
        this.pollForMessages()
          .catch((err) => log.error('Gmail poll error', { err }))
          .finally(() => {
            if (this.gmail) schedulePoll();
          });
      }, backoffMs);
    };

    await this.pollForMessages();
    schedulePoll();
  }

  async teardown(): Promise<void> {
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    this.gmail = null;
    this.oauth2Client = null;
    this.config = null;
    log.info('Gmail channel stopped');
  }

  isConnected(): boolean {
    return this.gmail !== null;
  }

  async deliver(_platformId: string, _threadId: string | null, message: OutboundMessage): Promise<string | undefined> {
    if (!this.gmail) {
      log.warn('Gmail not initialized, dropping reply');
      return undefined;
    }

    if (!this.lastThreadId) {
      log.warn('No email has been delivered yet, nothing to reply to');
      return undefined;
    }
    const threadId = this.lastThreadId;

    const meta = this.threadMeta.get(threadId);
    if (!meta) {
      log.warn('No thread metadata for reply, cannot send', { threadId });
      return undefined;
    }

    const text = extractText(message);
    if (text === null) return undefined;

    const subject = meta.subject.startsWith('Re:') ? meta.subject : `Re: ${meta.subject}`;

    // SAFETY: always self-addressed. This is the agent talking to the
    // mailbox owner, not a deliberate reply to the original sender — no
    // tool has ever existed for the agent to choose to email a third
    // party, so every response must land only in the owner's own inbox
    // (threaded into the same conversation for context). Addressing this
    // to meta.sender was the root cause of #incident-2026-09-15: hours of
    // auto-generated replies landing on newsletter senders, a talent-pool
    // system, a lottery marketing address, and a live Hetzner support
    // ticket — because every ordinary agent turn ends up here.
    const headers = [
      `To: ${this.userEmail}`,
      `From: ${this.userEmail}`,
      `Subject: ${subject}`,
      `In-Reply-To: ${meta.messageId}`,
      `References: ${meta.messageId}`,
      'Content-Type: text/plain; charset=utf-8',
      '',
      text,
    ].join('\r\n');

    const encodedMessage = Buffer.from(headers)
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');

    try {
      const res = await this.gmail.users.messages.send({
        userId: 'me',
        requestBody: { raw: encodedMessage, threadId },
      });
      log.info('Gmail note-to-self sent', { threadId });
      return res.data.id ?? undefined;
    } catch (err) {
      log.error('Failed to send Gmail note-to-self', { threadId, err });
      return undefined;
    }
  }

  // --- Private ---

  private buildQuery(): string {
    // -label:newsletter excludes anything the user has labeled "newsletter"
    // in Gmail itself (a normal Gmail filter rule) — keeps the exclusion
    // list entirely in Gmail's UI instead of hardcoded sender patterns here.
    // Harmless no-op until that label exists: Gmail just matches nothing for it.
    return 'is:unread category:primary -label:newsletter';
  }

  private async pollForMessages(): Promise<void> {
    if (!this.gmail) return;

    try {
      const res = await this.gmail.users.messages.list({
        userId: 'me',
        q: this.buildQuery(),
        maxResults: 10,
      });

      const messages = res.data.messages || [];

      for (const stub of messages) {
        if (!stub.id || this.processedIds.has(stub.id)) continue;
        this.processedIds.add(stub.id);

        await this.processMessage(stub.id);
      }

      // Cap processed ID set to prevent unbounded growth.
      if (this.processedIds.size > 5000) {
        const ids = [...this.processedIds];
        this.processedIds = new Set(ids.slice(ids.length - 2500));
      }

      this.consecutiveErrors = 0;
    } catch (err) {
      this.consecutiveErrors++;
      const backoffMs = Math.min(this.pollIntervalMs * Math.pow(2, this.consecutiveErrors), 30 * 60 * 1000);
      log.error('Gmail poll failed', { err, consecutiveErrors: this.consecutiveErrors, nextPollMs: backoffMs });
    }
  }

  private async processMessage(messageId: string): Promise<void> {
    if (!this.gmail || !this.config) return;

    const msg = await this.gmail.users.messages.get({ userId: 'me', id: messageId, format: 'full' });

    const headers = msg.data.payload?.headers || [];
    const getHeader = (name: string) => headers.find((h) => h.name?.toLowerCase() === name.toLowerCase())?.value || '';

    const from = getHeader('From');
    const subject = getHeader('Subject');
    const rfc2822MessageId = getHeader('Message-ID');
    const threadId = msg.data.threadId || messageId;
    const timestamp = new Date(parseInt(msg.data.internalDate || '0', 10)).toISOString();

    const senderMatch = from.match(/^(.+?)\s*<(.+?)>$/);
    const senderName = senderMatch ? senderMatch[1].replace(/"/g, '') : from;
    const senderEmail = senderMatch ? senderMatch[2] : from;

    // Skip emails from self (our own replies).
    if (senderEmail === this.userEmail) return;

    const body = this.extractTextBody(msg.data.payload);
    if (!body) {
      log.debug('Skipping email with no text body', { messageId, subject });
      return;
    }

    // Keep metadata for every thread we've seen — deliver() replies into
    // whichever one was most recently delivered (this.lastThreadId).
    this.threadMeta.set(threadId, {
      sender: senderEmail,
      senderName,
      subject,
      messageId: rfc2822MessageId,
    });
    this.lastThreadId = threadId;

    const content = `[Email from ${senderName} <${senderEmail}>]\nSubject: ${subject}\n\n${body}`;

    // platformId is the fixed mailbox address — every email is a message in
    // the one shared inbox conversation, not its own platform conversation.
    await this.config.onInbound(this.userEmail, null, {
      id: messageId,
      kind: 'chat',
      content: { text: content, senderId: senderEmail, senderName },
      timestamp,
      isGroup: false,
    });

    try {
      await this.gmail.users.messages.modify({
        userId: 'me',
        id: messageId,
        requestBody: { removeLabelIds: ['UNREAD'] },
      });
    } catch (err) {
      log.warn('Failed to mark email as read', { messageId, err });
    }

    log.info('Gmail email delivered', { threadId, from: senderName, subject });
  }

  private extractTextBody(payload: gmail_v1.Schema$MessagePart | undefined): string {
    if (!payload) return '';

    if (payload.mimeType === 'text/plain' && payload.body?.data) {
      return Buffer.from(payload.body.data, 'base64').toString('utf-8');
    }

    if (payload.parts) {
      for (const part of payload.parts) {
        if (part.mimeType === 'text/plain' && part.body?.data) {
          return Buffer.from(part.body.data, 'base64').toString('utf-8');
        }
      }
      for (const part of payload.parts) {
        const text = this.extractTextBody(part);
        if (text) return text;
      }
    }

    return '';
  }
}

function extractText(message: OutboundMessage): string | null {
  const content = message.content as Record<string, unknown> | string | undefined;
  if (typeof content === 'string') return content;
  if (content && typeof content === 'object' && typeof content.text === 'string') {
    return content.text;
  }
  return null;
}

function createAdapter(): ChannelAdapter | null {
  if (!hasCredentials()) {
    log.warn('Gmail: credentials not found in ~/.gmail-mcp/. Run /add-gmail to set up.');
    return null;
  }
  // Email isn't a real-time channel — poll every 5 minutes rather than every
  // 60 seconds. Gmail API quota is not the constraint (a poll costs a
  // handful of the ~1 billion daily quota units); this is just to avoid
  // pointless host load and container wakes for a non-urgent channel.
  return new GmailChannel(5 * 60 * 1000);
}

registerChannelAdapter('gmail', {
  factory: createAdapter,
  defaults: GMAIL_DEFAULTS,
});
