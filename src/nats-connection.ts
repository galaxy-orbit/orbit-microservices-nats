import { 
  NatsProtocolParser, 
  NatsProtocolEncoder, 
  type NatsServerInfo, 
  type NatsConnectOptions,
  type NatsParsedCommand,
  type NatsMessage,
} from './nats-protocol';

export interface NatsConnectionOptions {
  host?: string;
  port?: number;
  servers?: string[];
  user?: string;
  pass?: string;
  token?: string;
  name?: string;
  maxReconnectAttempts?: number;
  reconnectTimeWait?: number;
  reconnectJitter?: number;
  pingInterval?: number;
  maxPingOut?: number;
  timeout?: number;
  noEcho?: boolean;
}

type MessageHandler = (msg: NatsMessage) => void;
type EventHandler = () => void;
type ErrorHandler = (error: Error) => void;

interface NatsSocket {
  write(data: string | Uint8Array): number;
  end(): void;
}

export class NatsConnection {
  private options: Required<Pick<NatsConnectionOptions, 
    'host' | 'port' | 'maxReconnectAttempts' | 'reconnectTimeWait' | 
    'reconnectJitter' | 'pingInterval' | 'maxPingOut' | 'timeout'
  >> & NatsConnectionOptions;
  
  private socket: NatsSocket | null = null;
  private parser: NatsProtocolParser;
  private serverInfo: NatsServerInfo | null = null;
  
  private isConnected = false;
  private isConnecting = false;
  private isClosed = false;
  private reconnectAttempts = 0;
  
  private pingTimer: Timer | null = null;
  private pingsOut = 0;
  
  private messageHandlers: MessageHandler[] = [];
  private connectHandlers: EventHandler[] = [];
  private disconnectHandlers: EventHandler[] = [];
  private reconnectHandlers: EventHandler[] = [];
  private errorHandlers: ErrorHandler[] = [];
  
  private connectPromise: Promise<void> | null = null;
  private connectResolve: (() => void) | null = null;
  private connectReject: ((error: Error) => void) | null = null;

  constructor(options: NatsConnectionOptions = {}) {
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 4222,
      maxReconnectAttempts: options.maxReconnectAttempts ?? 10,
      reconnectTimeWait: options.reconnectTimeWait ?? 2000,
      reconnectJitter: options.reconnectJitter ?? 100,
      pingInterval: options.pingInterval ?? 120000,
      maxPingOut: options.maxPingOut ?? 2,
      timeout: options.timeout ?? 20000,
      ...options,
    };
    this.parser = new NatsProtocolParser();
  }

  async connect(): Promise<void> {
    if (this.isConnected) return;
    if (this.isConnecting && this.connectPromise) {
      return this.connectPromise;
    }
    
    this.isConnecting = true;
    this.isClosed = false;
    
    this.connectPromise = new Promise((resolve, reject) => {
      this.connectResolve = resolve;
      this.connectReject = reject;
    });
    
    try {
      await this.doConnect();
      return this.connectPromise;
    } catch (error) {
      this.isConnecting = false;
      throw error;
    }
  }

  private async doConnect(): Promise<void> {
    const { host, port } = this.options;
    
    this.parser.reset();
    
    try {
      const socket = await Bun.connect({
        hostname: host,
        port: port,
        socket: {
          open: () => this.onSocketOpen(),
          data: (_, data) => this.onSocketData(data),
          error: (_, error) => this.onSocketError(error),
          close: () => this.onSocketClose(),
        },
      });
      this.socket = socket as unknown as NatsSocket;
    } catch (error: any) {
      this.handleConnectError(error);
    }
  }

  private onSocketOpen(): void {
    this.pingsOut = 0;
  }

  private onSocketData(data: Buffer): void {
    const commands = this.parser.feed(data);
    
    for (const cmd of commands) {
      this.handleCommand(cmd);
    }
  }

  private handleCommand(cmd: NatsParsedCommand): void {
    switch (cmd.type) {
      case 'INFO':
        this.handleInfo(cmd.data);
        break;
      case 'MSG':
        this.handleMessage(cmd.message);
        break;
      case 'PING':
        this.write(NatsProtocolEncoder.pong());
        break;
      case 'PONG':
        this.pingsOut = 0;
        break;
      case '+OK':
        break;
      case '-ERR':
        this.handleError(new Error(`NATS Error: ${cmd.message}`));
        break;
    }
  }

  private handleInfo(info: NatsServerInfo): void {
    this.serverInfo = info;
    
    const connectOptions: NatsConnectOptions = {
      verbose: false,
      pedantic: false,
      echo: !this.options.noEcho,
    };
    
    if (this.options.user && this.options.pass) {
      connectOptions.user = this.options.user;
      connectOptions.pass = this.options.pass;
    } else if (this.options.token) {
      connectOptions.auth_token = this.options.token;
    }
    
    if (this.options.name) {
      connectOptions.name = this.options.name;
    }
    
    this.write(NatsProtocolEncoder.connect(connectOptions));
    
    this.isConnected = true;
    this.isConnecting = false;
    this.reconnectAttempts = 0;
    
    this.startPingTimer();
    
    if (this.connectResolve) {
      this.connectResolve();
      this.connectResolve = null;
      this.connectReject = null;
    }
    
    for (const handler of this.connectHandlers) {
      handler();
    }
  }

  private handleMessage(msg: NatsMessage): void {
    for (const handler of this.messageHandlers) {
      try {
        handler(msg);
      } catch (error) {
        console.error('[NatsConnection] Message handler error:', error);
      }
    }
  }

  private handleError(error: Error): void {
    for (const handler of this.errorHandlers) {
      handler(error);
    }
  }

  private handleConnectError(error: Error): void {
    if (this.connectReject) {
      this.connectReject(error);
      this.connectResolve = null;
      this.connectReject = null;
    }
    this.isConnecting = false;
  }

  private onSocketError(error: Error): void {
    console.error('[NatsConnection] Socket error:', error);
    this.handleError(error);
  }

  private onSocketClose(): void {
    const wasConnected = this.isConnected;
    this.isConnected = false;
    this.stopPingTimer();
    
    if (wasConnected) {
      for (const handler of this.disconnectHandlers) {
        handler();
      }
    }
    
    if (!this.isClosed && this.reconnectAttempts < this.options.maxReconnectAttempts) {
      this.scheduleReconnect();
    } else if (this.connectReject) {
      this.connectReject(new Error('Connection closed'));
      this.connectResolve = null;
      this.connectReject = null;
    }
  }

  private async scheduleReconnect(): Promise<void> {
    this.reconnectAttempts++;
    const delay = this.options.reconnectTimeWait + 
      Math.random() * this.options.reconnectJitter;
    
    await new Promise(resolve => setTimeout(resolve, delay));
    
    if (!this.isClosed) {
      try {
        this.connectPromise = new Promise((resolve, reject) => {
          this.connectResolve = resolve;
          this.connectReject = reject;
        });
        
        await this.doConnect();
        await this.connectPromise;
        
        for (const handler of this.reconnectHandlers) {
          handler();
        }
      } catch (error) {
        if (this.reconnectAttempts < this.options.maxReconnectAttempts) {
          this.scheduleReconnect();
        }
      }
    }
  }

  private startPingTimer(): void {
    this.stopPingTimer();
    
    this.pingTimer = setInterval(() => {
      if (this.pingsOut >= this.options.maxPingOut) {
        this.handleError(new Error('Stale connection - max pings exceeded'));
        this.socket?.end();
        return;
      }
      
      this.pingsOut++;
      this.write(NatsProtocolEncoder.ping());
    }, this.options.pingInterval);
  }

  private stopPingTimer(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  write(data: string): boolean {
    if (!this.socket || !this.isConnected) {
      return false;
    }
    
    try {
      this.socket.write(data);
      return true;
    } catch (error) {
      console.error('[NatsConnection] Write error:', error);
      return false;
    }
  }

  publish(subject: string, payload: string, replyTo?: string): boolean {
    return this.write(NatsProtocolEncoder.pub(subject, payload, replyTo));
  }

  subscribe(subject: string, sid: number, queue?: string): boolean {
    return this.write(NatsProtocolEncoder.sub(subject, sid, queue));
  }

  unsubscribe(sid: number, maxMsgs?: number): boolean {
    return this.write(NatsProtocolEncoder.unsub(sid, maxMsgs));
  }

  onMessage(handler: MessageHandler): void {
    this.messageHandlers.push(handler);
  }

  onConnect(handler: EventHandler): void {
    this.connectHandlers.push(handler);
  }

  onDisconnect(handler: EventHandler): void {
    this.disconnectHandlers.push(handler);
  }

  onReconnect(handler: EventHandler): void {
    this.reconnectHandlers.push(handler);
  }

  onError(handler: ErrorHandler): void {
    this.errorHandlers.push(handler);
  }

  getServerInfo(): NatsServerInfo | null {
    return this.serverInfo;
  }

  connected(): boolean {
    return this.isConnected;
  }

  async close(): Promise<void> {
    this.isClosed = true;
    this.stopPingTimer();
    
    if (this.socket) {
      this.socket.end();
      this.socket = null;
    }
    
    this.isConnected = false;
    this.isConnecting = false;
    this.messageHandlers = [];
    this.connectHandlers = [];
    this.disconnectHandlers = [];
    this.reconnectHandlers = [];
    this.errorHandlers = [];
  }

  async flush(): Promise<void> {
    if (!this.isConnected) return;
    
    return new Promise((resolve) => {
      this.write(NatsProtocolEncoder.ping());
      setTimeout(resolve, 1000);
    });
  }
}
