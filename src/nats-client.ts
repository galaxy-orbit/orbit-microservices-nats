import { ClientProxy, type ReadPacket, type WritePacket } from '@galaxy-stack/orbit-microservices';
import { NatsConnection, type NatsConnectionOptions } from './nats-connection';
import type { NatsMessage } from './nats-protocol';

export interface NatsClientOptions extends NatsConnectionOptions {
  serializer?: (data: any) => string;
  deserializer?: (data: string) => any;
  timeout?: number;
}

interface NatsPendingRequest {
  resolve: (value: any) => void;
  reject: (error: Error) => void;
  callback?: (packet: WritePacket) => void;
  timer: Timer;
  sid: number;
}

export class NatsClient extends ClientProxy {
  private readonly options: NatsClientOptions;
  private connection: NatsConnection;
  private natsRequests: Map<string, NatsPendingRequest> = new Map();
  private sidCounter = 1;
  private inboxPrefix: string;
  private inboxSid: number = 0;
  private isInboxSubscribed = false;

  constructor(options: NatsClientOptions = {}) {
    super();
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 4222,
      maxReconnectAttempts: options.maxReconnectAttempts ?? 10,
      reconnectTimeWait: options.reconnectTimeWait ?? 2000,
      timeout: options.timeout ?? 30000,
      serializer: options.serializer || JSON.stringify,
      deserializer: options.deserializer || JSON.parse,
      ...options,
    };
    
    this.connection = new NatsConnection(this.options);
    this.inboxPrefix = `_INBOX.${this.generateId()}`;
  }

  async connect(): Promise<void> {
    if (this.connection.connected()) {
      this.isConnected = true;
      return;
    }
    
    this.setupConnectionHandlers();
    await this.connection.connect();
    await this.subscribeToInbox();
    this.isConnected = true;
  }

  private setupConnectionHandlers(): void {
    this.connection.onMessage((msg) => this.processNatsMessage(msg));
    
    this.connection.onReconnect(() => {
      console.log('[NatsClient] Reconnected, resubscribing to inbox...');
      this.isInboxSubscribed = false;
      this.subscribeToInbox();
    });
    
    this.connection.onError((error) => {
      console.error('[NatsClient] Connection error:', error);
    });
    
    this.connection.onDisconnect(() => {
      this.isConnected = false;
      for (const [id, pending] of this.natsRequests) {
        clearTimeout(pending.timer);
        pending.reject(new Error('Connection lost'));
      }
      this.natsRequests.clear();
    });
  }

  private async subscribeToInbox(): Promise<void> {
    if (this.isInboxSubscribed) return;
    
    this.inboxSid = this.sidCounter++;
    this.connection.subscribe(`${this.inboxPrefix}.*`, this.inboxSid);
    this.isInboxSubscribed = true;
  }

  private processNatsMessage(msg: NatsMessage): void {
    const { subject, payload } = msg;
    
    if (!subject.startsWith(this.inboxPrefix)) {
      return;
    }
    
    const requestId = subject.slice(this.inboxPrefix.length + 1);
    const pending = this.natsRequests.get(requestId);
    
    if (!pending) {
      return;
    }
    
    this.natsRequests.delete(requestId);
    clearTimeout(pending.timer);
    
    try {
      const response = payload ? this.options.deserializer!(payload) : {};
      
      if (pending.callback) {
        if (response.error) {
          pending.callback({ err: response.error.message || 'Unknown error', response: null });
        } else {
          pending.callback({ response: response.response });
        }
      } else {
        if (response.error) {
          pending.reject(new Error(response.error.message || 'Unknown error'));
        } else {
          pending.resolve(response.response);
        }
      }
    } catch (error: any) {
      if (pending.callback) {
        pending.callback({ err: `Failed to parse response: ${error.message}`, response: null });
      } else {
        pending.reject(new Error(`Failed to parse response: ${error.message}`));
      }
    }
  }

  protected publish(packet: ReadPacket, callback: (packet: WritePacket) => void): () => void {
    const requestId = this.generateId();
    const replySubject = `${this.inboxPrefix}.${requestId}`;
    const requestSubject = this.getRequestSubject(packet.pattern);
    const payload = this.options.serializer!(packet.data);

    const timer = setTimeout(() => {
      const pending = this.natsRequests.get(requestId);
      if (pending) {
        this.natsRequests.delete(requestId);
        callback({ err: 'Request timeout', response: null });
      }
    }, this.options.timeout!);

    this.natsRequests.set(requestId, {
      resolve: () => {},
      reject: () => {},
      callback,
      timer,
      sid: this.inboxSid,
    });

    this.connection.publish(requestSubject, payload, replySubject);

    return () => {
      const pending = this.natsRequests.get(requestId);
      if (pending) {
        clearTimeout(pending.timer);
        this.natsRequests.delete(requestId);
      }
    };
  }

  protected async dispatchEvent(packet: ReadPacket): Promise<void> {
    await this.connect();
    
    const subject = this.getEventSubject(packet.pattern);
    const payload = this.options.serializer!(packet.data);
    
    this.connection.publish(subject, payload);
  }

  request<TResult = any>(pattern: any, data: any, timeout?: number): Promise<TResult> {
    return new Promise(async (resolve, reject) => {
      try {
        await this.connect();

        const requestId = this.generateId();
        const replySubject = `${this.inboxPrefix}.${requestId}`;
        const requestSubject = this.getRequestSubject(pattern);
        const payload = this.options.serializer!(data);
        const requestTimeout = timeout || this.options.timeout!;

        const timer = setTimeout(() => {
          const pending = this.natsRequests.get(requestId);
          if (pending) {
            this.natsRequests.delete(requestId);
            reject(new Error('Request timeout'));
          }
        }, requestTimeout);

        this.natsRequests.set(requestId, {
          resolve,
          reject,
          timer,
          sid: this.inboxSid,
        });

        this.connection.publish(requestSubject, payload, replySubject);
      } catch (error) {
        reject(error);
      }
    });
  }

  private getRequestSubject(pattern: string | object): string {
    const patternStr = typeof pattern === 'object' 
      ? JSON.stringify(pattern) 
      : pattern;
    return `orbit.request.${this.normalizeSubject(patternStr)}`;
  }

  private getEventSubject(pattern: string | object): string {
    const patternStr = typeof pattern === 'object' 
      ? JSON.stringify(pattern) 
      : pattern;
    return `orbit.event.${this.normalizeSubject(patternStr)}`;
  }

  private normalizeSubject(pattern: string): string {
    return pattern
      .replace(/\//g, '.')
      .replace(/[{}":]/g, '')
      .replace(/,/g, '.');
  }

  async close(): Promise<void> {
    for (const [id, pending] of this.natsRequests) {
      clearTimeout(pending.timer);
      pending.reject(new Error('Client closed'));
    }
    this.natsRequests.clear();
    this.isInboxSubscribed = false;
    this.isConnected = false;

    await this.connection.close();
  }
}
