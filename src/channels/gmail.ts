/**
 * Gmail channel — polls the Gmail inbox for new primary/unread mail and
 * routes it through the normal v2 inbound path. Ingest-only: `deliver()` is
 * a hard no-op and sends nothing, on purpose.
 *
 * SAFETY: no automatic outbound send exists on this channel. #incident-2026-
 * 09-15: deliver() used to email whoever last sent a message in (hours of
 * auto-generated replies hit a newsletter, a talent-pool system, a lottery
 * marketing address, GitHub, and a live Hetzner support ticket, plus a
 * mailer-daemon bounce loop). A follow-up fix that self-addressed the same
 * replies as "notes to the owner" was ALSO rejected by the user: an
 * automatic summary email is still an unapproved send and still just adds
 * inbox noise. The user's explicit rule: no message of any kind may go out
 * automatically, on any channel, without their explicit approval or
 * explicit instruction for that specific send. Do not add any send path
 * back to this file without that approval design in place first — see
 * `deliver()`'s own docstring.
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
 * with its sender/subject, same as the original NanoClaw v1 design. Mentions
 * are declared 'never', so there is no auto-wire-on-first-contact flow here
 * — the operator wires this one conversation once via `ncl`.
 */
import fs from 'fs';
import os from 'os';
import path from 'path';

import { google, gmail_v1 } from 'googleapis';
import { OAuth2Client } from 'google-auth-library';

import { log } from '../log.js';
import { registerChannelAdapter } from './channel-registry.js';
import type { ChannelAdapter, ChannelDefaults, ChannelSetup, OutboundMessage } from './adapter.js';

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

  /**
   * SAFETY: hard no-op. Never sends anything — not to the original sender,
   * not as a self-addressed note, not anywhere. #incident-2026-09-15: this
   * used to reply to whoever last emailed in (hours of auto-generated
   * replies hit a newsletter, a talent-pool system, a lottery marketing
   * address, and a live Hetzner support ticket). The follow-up fix
   * (self-addressed "notes") was ALSO rejected by the user — an automatic
   * summary email is still an unapproved send and still just adds inbox
   * noise. Per the user's explicit rule: no message of any kind may go out
   * automatically, on any channel, without their explicit approval or
   * explicit instruction for that specific send. Do not restore any send
   * path here without that approval design in place first.
   */
  async deliver(_platformId: string, _threadId: string | null, _message: OutboundMessage): Promise<string | undefined> {
    log.info('Gmail outbound send suppressed (sending is disabled pending an explicit approval mechanism)');
    return undefined;
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
