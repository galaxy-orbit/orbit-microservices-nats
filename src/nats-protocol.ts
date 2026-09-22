/**
 * NATS Text Protocol Parser and Encoder
 * 
 * NATS uses a simple text-based protocol with the following commands:
 * - INFO: Server info (sent on connect)
 * - CONNECT: Client connection options
 * - PUB: Publish a message
 * - SUB: Subscribe to a subject
 * - UNSUB: Unsubscribe
 * - MSG: Message received from subscription
 * - PING/PONG: Keepalive
 * - +OK/-ERR: Response status
 */

export interface NatsServerInfo {
  server_id: string;
  server_name?: string;
  version: string;
  proto: number;
  go?: string;
  host: string;
  port: number;
  headers?: boolean;
  max_payload: number;
  client_id?: number;
  auth_required?: boolean;
  tls_required?: boolean;
  tls_verify?: boolean;
  connect_urls?: string[];
}

export interface NatsConnectOptions {
  verbose?: boolean;
  pedantic?: boolean;
  tls_required?: boolean;
  auth_token?: string;
  user?: string;
  pass?: string;
  name?: string;
  lang?: string;
  version?: string;
  protocol?: number;
  echo?: boolean;
  sig?: string;
  jwt?: string;
  no_responders?: boolean;
  headers?: boolean;
}

export interface NatsMessage {
  subject: string;
  sid: number;
  replyTo?: string;
  payload: string;
  byteCount: number;
}

export type NatsParsedCommand = 
  | { type: 'INFO'; data: NatsServerInfo }
  | { type: 'MSG'; message: NatsMessage }
  | { type: 'PING' }
  | { type: 'PONG' }
  | { type: '+OK' }
  | { type: '-ERR'; message: string }
  | { type: 'HMSG'; message: NatsMessage; headers: Record<string, string> };

const CRLF = '\r\n';
const INFO_PREFIX = 'INFO ';
const MSG_PREFIX = 'MSG ';
const HMSG_PREFIX = 'HMSG ';
const ERR_PREFIX = '-ERR ';

export class NatsProtocolParser {
  private buffer = '';
  private pendingMessage: { header: string; byteCount: number } | null = null;

  reset(): void {
    this.buffer = '';
    this.pendingMessage = null;
  }

  feed(data: string | Buffer): NatsParsedCommand[] {
    this.buffer += typeof data === 'string' ? data : data.toString();
    return this.parse();
  }

  private parse(): NatsParsedCommand[] {
    const commands: NatsParsedCommand[] = [];
    
    while (true) {
      if (this.pendingMessage) {
        const { header, byteCount } = this.pendingMessage;
        
        if (this.buffer.length < byteCount + 2) {
          break;
        }
        
        const payload = this.buffer.slice(0, byteCount);
        this.buffer = this.buffer.slice(byteCount + 2);
        this.pendingMessage = null;
        
        const message = this.parseMsgHeader(header, payload);
        if (message) {
          commands.push({ type: 'MSG', message });
        }
        continue;
      }
      
      const crlfIndex = this.buffer.indexOf(CRLF);
      if (crlfIndex === -1) {
        break;
      }
      
      const line = this.buffer.slice(0, crlfIndex);
      this.buffer = this.buffer.slice(crlfIndex + 2);
      
      const command = this.parseCommand(line);
      if (command) {
        if (command.type === 'MSG_PENDING') {
          this.pendingMessage = { 
            header: line, 
            byteCount: (command as any).byteCount 
          };
        } else {
          commands.push(command as NatsParsedCommand);
        }
      }
    }
    
    return commands;
  }

  private parseCommand(line: string): NatsParsedCommand | { type: 'MSG_PENDING'; byteCount: number } | null {
    if (line.startsWith(INFO_PREFIX)) {
      try {
        const jsonStr = line.slice(INFO_PREFIX.length);
        const data = JSON.parse(jsonStr) as NatsServerInfo;
        return { type: 'INFO', data };
      } catch {
        return null;
      }
    }
    
    if (line.startsWith(MSG_PREFIX)) {
      const parts = line.slice(MSG_PREFIX.length).split(' ');
      const byteCount = parseInt(parts[parts.length - 1], 10);
      return { type: 'MSG_PENDING', byteCount };
    }
    
    if (line === 'PING') {
      return { type: 'PING' };
    }
    
    if (line === 'PONG') {
      return { type: 'PONG' };
    }
    
    if (line === '+OK') {
      return { type: '+OK' };
    }
    
    if (line.startsWith(ERR_PREFIX)) {
      const message = line.slice(ERR_PREFIX.length).replace(/^'|'$/g, '');
      return { type: '-ERR', message };
    }
    
    return null;
  }

  private parseMsgHeader(header: string, payload: string): NatsMessage | null {
    const parts = header.slice(MSG_PREFIX.length).split(' ');
    
    if (parts.length === 3) {
      return {
        subject: parts[0],
        sid: parseInt(parts[1], 10),
        byteCount: parseInt(parts[2], 10),
        payload,
      };
    }
    
    if (parts.length === 4) {
      return {
        subject: parts[0],
        sid: parseInt(parts[1], 10),
        replyTo: parts[2],
        byteCount: parseInt(parts[3], 10),
        payload,
      };
    }
    
    return null;
  }
}

export class NatsProtocolEncoder {
  static connect(options: NatsConnectOptions): string {
    const connectPayload: NatsConnectOptions = {
      verbose: false,
      pedantic: false,
      lang: 'orbit',
      version: '1.0.0',
      protocol: 1,
      ...options,
    };
    return `CONNECT ${JSON.stringify(connectPayload)}${CRLF}`;
  }

  static pub(subject: string, payload: string, replyTo?: string): string {
    const byteLength = Buffer.byteLength(payload);
    if (replyTo) {
      return `PUB ${subject} ${replyTo} ${byteLength}${CRLF}${payload}${CRLF}`;
    }
    return `PUB ${subject} ${byteLength}${CRLF}${payload}${CRLF}`;
  }

  static sub(subject: string, sid: number, queue?: string): string {
    if (queue) {
      return `SUB ${subject} ${queue} ${sid}${CRLF}`;
    }
    return `SUB ${subject} ${sid}${CRLF}`;
  }

  static unsub(sid: number, maxMsgs?: number): string {
    if (maxMsgs !== undefined) {
      return `UNSUB ${sid} ${maxMsgs}${CRLF}`;
    }
    return `UNSUB ${sid}${CRLF}`;
  }

  static ping(): string {
    return `PING${CRLF}`;
  }

  static pong(): string {
    return `PONG${CRLF}`;
  }
}

export function matchSubject(pattern: string, subject: string): boolean {
  const patternParts = pattern.split('.');
  const subjectParts = subject.split('.');
  
  let pi = 0;
  let si = 0;
  
  while (pi < patternParts.length && si < subjectParts.length) {
    const pp = patternParts[pi];
    
    if (pp === '>') {
      return true;
    }
    
    if (pp === '*') {
      pi++;
      si++;
      continue;
    }
    
    if (pp !== subjectParts[si]) {
      return false;
    }
    
    pi++;
    si++;
  }
  
  return pi === patternParts.length && si === subjectParts.length;
}
