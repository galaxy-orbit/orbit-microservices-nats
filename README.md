# @galaxy-stack/orbit-microservices-nats

[![npm version](https://img.shields.io/npm/v/@galaxy-stack/orbit-microservices-nats.svg)](https://www.npmjs.com/package/@galaxy-stack/orbit-microservices-nats)
[![docs](https://img.shields.io/badge/docs-galaxy--orbit--framework.vercel.app-blue)](https://galaxy-orbit-framework.vercel.app)

Part of the [Orbit framework](https://github.com/galaxy-orbit/packages) — a NestJS-style backend framework for [Bun](https://bun.sh).

## Installation

```bash
bun add @galaxy-stack/orbit-microservices-nats
```

# @galaxy-stack/orbit-microservices-nats

> **Status**: ✅ Full Implementation - Complete NATS text protocol với Bun native TCP

## Mô tả

NATS transport implementation cho Orbit microservices với queue groups và wildcard subscriptions.

## Tính năng

- Complete NATS text protocol parser/encoder
- Queue groups cho load balancing
- Wildcard subject matching (`*`, `>`)
- Automatic reconnection với exponential backoff
- Request-response (RPC) pattern với INBOX subjects
- Event-driven pattern với pub/sub
- Connection state management
- Graceful shutdown

## Cài đặt

```bash
bun add @galaxy-stack/orbit-microservices-nats
```

## Sử dụng

### NATS Server

```typescript
import { BunFactory } from '@galaxy-stack/orbit-core';
import '@galaxy-stack/orbit-microservices-nats';

const app = await BunFactory.createMicroservice(AppModule, {
  transport: 'NATS',
  options: {
    servers: ['nats://localhost:4222'],
    queue: 'workers',  // Queue group for load balancing
  },
});

await app.listen();
// [NatsServer] Connected to NATS
// [NatsServer] Listening on nats://localhost:4222
```

### NATS Client

```typescript
import { MicroservicesModule } from '@galaxy-stack/orbit-microservices';
import '@galaxy-stack/orbit-microservices-nats';

@Module({
  imports: [
    MicroservicesModule.register({
      name: 'NATS_SERVICE',
      transport: 'NATS',
      options: {
        servers: ['nats://localhost:4222'],
      },
    }),
  ],
})
class ClientModule {}
```

### Message Handlers

```typescript
import { Controller, MessagePattern, EventPattern } from '@galaxy-stack/orbit-microservices';

@Controller()
class OrderController {
  @MessagePattern('orders.create')
  createOrder(data: { productId: number; quantity: number }) {
    return { orderId: Date.now(), ...data };
  }

  @EventPattern('orders.shipped')
  handleOrderShipped(data: { orderId: number }) {
    console.log('Order shipped:', data.orderId);
  }

  // Wildcard pattern
  @EventPattern('orders.*')
  handleAllOrderEvents(data: any) {
    console.log('Order event:', data);
  }
}
```

### Client Usage

```typescript
@Injectable()
class OrderService {
  constructor(
    @Inject('NATS_SERVICE') private client: ClientProxy
  ) {}

  async createOrder(productId: number, quantity: number) {
    return this.client.send('orders.create', { productId, quantity });
  }

  notifyShipped(orderId: number): void {
    this.client.emit('orders.shipped', { orderId });
  }
}
```

## Options

### NatsServerOptions

```typescript
interface NatsServerOptions {
  servers?: string | string[];    // NATS servers
  host?: string;                  // Default: 'localhost'
  port?: number;                  // Default: 4222
  user?: string;                  // Username
  pass?: string;                  // Password
  token?: string;                 // Auth token
  queue?: string;                 // Queue group name
  maxReconnectAttempts?: number;  // Default: 10
  reconnectTimeWait?: number;     // Default: 2000ms
  connectTimeout?: number;        // Default: 10000ms
  requestTimeout?: number;        // Default: 30000ms
}
```

### NatsClientOptions

```typescript
interface NatsClientOptions extends NatsServerOptions {
  requestTimeout?: number; // Default: 30000ms
}
```

## NATS Protocol

Full implementation of NATS text-based protocol:

### Commands

| Command | Format | Description |
|---------|--------|-------------|
| CONNECT | `CONNECT {json}\r\n` | Connection handshake |
| SUB | `SUB subject [queue] sid\r\n` | Subscribe to subject |
| UNSUB | `UNSUB sid [max]\r\n` | Unsubscribe |
| PUB | `PUB subject [reply] size\r\npayload\r\n` | Publish message |
| PING | `PING\r\n` | Heartbeat request |
| PONG | `PONG\r\n` | Heartbeat response |

### Server Responses

| Response | Format | Description |
|----------|--------|-------------|
| INFO | `INFO {json}\r\n` | Server info |
| MSG | `MSG subject sid [reply] size\r\npayload\r\n` | Incoming message |
| +OK | `+OK\r\n` | Command acknowledged |
| -ERR | `-ERR 'message'\r\n` | Error response |

## Subject Naming

| Type | Pattern |
|------|---------|
| Request | `orbit.request.{pattern}` |
| Reply | `_INBOX.{clientId}.{requestId}` |
| Event | `orbit.event.{pattern}` |

## Queue Groups

Queue groups enable load balancing across multiple consumers:

```typescript
// Multiple instances with same queue group
options: {
  queue: 'order-workers',
}

// Only ONE instance receives each message
```

## Wildcard Subscriptions

NATS supports two wildcards:

| Wildcard | Description | Example |
|----------|-------------|---------|
| `*` | Match single token | `orders.*` matches `orders.create` |
| `>` | Match multiple tokens | `orders.>` matches `orders.us.create` |

## NatsConnection

Low-level NATS connection với event handlers:

```typescript
import { NatsConnection } from '@galaxy-stack/orbit-microservices-nats';

const conn = new NatsConnection({
  host: 'localhost',
  port: 4222,
});

conn.onMessage((subject, data, replyTo, sid) => {
  console.log(`Received on ${subject}: ${data}`);
});

conn.onError((error) => {
  console.error('Connection error:', error);
});

conn.onReconnect(() => {
  console.log('Reconnected to NATS');
});

await conn.connect();
await conn.subscribe('my.subject', 'my-queue');
await conn.publish('my.subject', 'Hello!');
await conn.quit();
```

## Error Handling

```typescript
try {
  const result = await client.send('orders.validate', { orderId: 123 });
} catch (error) {
  console.error(error.message); // 'Order not found'
}
```

## Reconnection

Automatic reconnection với configurable retry:

- Retry attempts: configurable (default: 10)
- Retry delay: configurable (default: 2000ms)
- Automatic resubscription after reconnect

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Application                          │
├─────────────────────────────────────────────────────────┤
│      NatsServer          │         NatsClient          │
├─────────────────────────────────────────────────────────┤
│              NatsConnection (Protocol Handler)          │
├─────────────────────────────────────────────────────────┤
│              NATS Text Protocol Parser                  │
├─────────────────────────────────────────────────────────┤
│                   Bun.connect()                         │
└─────────────────────────────────────────────────────────┘
```
