import { registerTransport, Transport } from '@galaxy-stack/orbit-microservices';
import { NatsServer, type NatsServerOptions } from './nats-server';
import { NatsClient, type NatsClientOptions } from './nats-client';

registerTransport(Transport.NATS, NatsServer, NatsClient);

export { NatsServer, type NatsServerOptions } from './nats-server';
export { NatsClient, type NatsClientOptions } from './nats-client';
export { NatsConnection, type NatsConnectionOptions } from './nats-connection';
export { 
  NatsProtocolParser,
  NatsProtocolEncoder,
  matchSubject,
  type NatsServerInfo,
  type NatsConnectOptions,
  type NatsMessage,
  type NatsParsedCommand,
} from './nats-protocol';
