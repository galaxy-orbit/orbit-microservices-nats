import { Server, type TransportOptions } from '@galaxy-stack/orbit-microservices';
import { NatsConnection, type NatsConnectionOptions } from './nats-connection';
import { matchSubject, type NatsMessage } from './nats-protocol';

export interface NatsServerOptions extends TransportOptions, NatsConnectionOptions {
  queue?: string;
  serializer?: (data: any) => string;
  deserializer?: (data: string) => any;
}

interface NatsSubscription {
  sid: number;
  subject: string;
  queue?: string;
  handler: (data: any) => Promise<any>;
  isEvent: boolean;
}

export class NatsServer extends Server {
  private readonly options: NatsServerOptions;
  private connection: NatsConnection;
  private subscriptions: Map<number, NatsSubscription> = new Map();
  private subjectToSid: Map<string, number> = new Map();
  private eventPatterns: Set<string> = new Set();
  private sidCounter = 1;
  private isListening = false;

  constructor(options: NatsServerOptions = {}) {
    super();
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 4222,
      queue: options.queue || 'orbit-workers',
      maxReconnectAttempts: options.maxReconnectAttempts ?? 10,
      reconnectTimeWait: options.reconnectTimeWait ?? 2000,
      serializer: options.serializer || JSON.stringify,
      deserializer: options.deserializer || JSON.parse,
      ...options,
    };
    
    this.connection = new NatsConnection(this.options);
  }

  addHandler(pattern: string | object, handler: (data: any) => Promise<any>, isEventHandler = false): void {
    super.addHandler(pattern, handler, isEventHandler);
    if (isEventHandler) {
      const key = this.normalizePattern(pattern);
      this.eventPatterns.add(key);
    }
  }

  async listen(callback?: () => void): Promise<void> {
    try {
      this.setupConnectionHandlers();
      await this.connection.connect();
      this.isListening = true;
      await this.bindAllHandlers();
      callback?.();
    } catch (error) {
      console.error('[NatsServer] Failed to start:', error);
      throw error;
    }
  }

  private setupConnectionHandlers(): void {
    this.connection.onMessage((msg) => this.processNatsMessage(msg));
    
    this.connection.onReconnect(() => {
      console.log('[NatsServer] Reconnected, resubscribing...');
      this.resubscribeAll();
    });
    
    this.connection.onError((error) => {
      console.error('[NatsServer] Connection error:', error);
    });
    
    this.connection.onDisconnect(() => {
      console.log('[NatsServer] Disconnected from NATS');
    });
  }

  private async bindAllHandlers(): Promise<void> {
    for (const [patternKey, handler] of this.messageHandlers) {
      const isEvent = this.eventPatterns.has(patternKey);
      const subject = isEvent 
        ? this.getEventSubject(patternKey)
        : this.getRequestSubject(patternKey);
      this.subscribeToSubject(subject, this.options.queue, handler, isEvent);
    }
  }

  private subscribeToSubject(
    subject: string,
    queue: string | undefined,
    handler: (data: any) => Promise<any>,
    isEvent: boolean
  ): void {
    const sid = this.sidCounter++;
    
    this.subscriptions.set(sid, {
      sid,
      subject,
      queue,
      handler,
      isEvent,
    });
    this.subjectToSid.set(subject, sid);

    this.connection.subscribe(subject, sid, queue);
  }

  private resubscribeAll(): void {
    for (const sub of this.subscriptions.values()) {
      this.connection.subscribe(sub.subject, sub.sid, sub.queue);
    }
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

  private async processNatsMessage(msg: NatsMessage): Promise<void> {
    const { subject, replyTo, payload } = msg;
    
    let subscription: NatsSubscription | undefined;
    
    for (const sub of this.subscriptions.values()) {
      if (matchSubject(sub.subject, subject)) {
        subscription = sub;
        break;
      }
    }
    
    if (!subscription) {
      return;
    }

    try {
      const data = payload ? this.options.deserializer!(payload) : {};
      const result = await subscription.handler(data);
      
      if (replyTo && !subscription.isEvent) {
        const response = this.options.serializer!({ response: result });
        this.connection.publish(replyTo, response);
      }
    } catch (error: any) {
      console.error('[NatsServer] Handler error:', error);
      
      if (replyTo && !subscription.isEvent) {
        const errorResponse = this.options.serializer!({ 
          error: {
            message: error.message || 'Unknown error',
            code: error.code || 'INTERNAL_ERROR',
          }
        });
        this.connection.publish(replyTo, errorResponse);
      }
    }
  }

  async close(): Promise<void> {
    this.isListening = false;
    
    for (const sub of this.subscriptions.values()) {
      this.connection.unsubscribe(sub.sid);
    }
    this.subscriptions.clear();
    this.subjectToSid.clear();

    await this.connection.close();
  }
}
