import { describe, test, expect } from 'bun:test';
import { NatsProtocolParser, NatsProtocolEncoder, matchSubject } from './nats-protocol';

describe('NatsProtocolEncoder', () => {
  test('connect serializes options as JSON', () => {
    const cmd = NatsProtocolEncoder.connect({ user: 'u', pass: 'p' });
    expect(cmd).toMatch(/^CONNECT \{.*\}\r\n$/);
    expect(cmd).toContain('"lang":"orbit"');
  });

  test('pub without replyTo', () => {
    const cmd = NatsProtocolEncoder.pub('orders.created', '{"id":1}');
    expect(cmd).toBe('PUB orders.created 8\r\n{"id":1}\r\n');
  });

  test('pub with replyTo', () => {
    const cmd = NatsProtocolEncoder.pub('orders.created', 'payload', 'inbox.1');
    expect(cmd).toBe('PUB orders.created inbox.1 7\r\npayload\r\n');
  });

  test('sub with and without queue group', () => {
    expect(NatsProtocolEncoder.sub('a.b', 1)).toBe('SUB a.b 1\r\n');
    expect(NatsProtocolEncoder.sub('a.b', 2, 'workers')).toBe('SUB a.b workers 2\r\n');
  });

  test('unsub with and without maxMsgs', () => {
    expect(NatsProtocolEncoder.unsub(1)).toBe('UNSUB 1\r\n');
    expect(NatsProtocolEncoder.unsub(1, 5)).toBe('UNSUB 1 5\r\n');
  });

  test('ping/pong', () => {
    expect(NatsProtocolEncoder.ping()).toBe('PING\r\n');
    expect(NatsProtocolEncoder.pong()).toBe('PONG\r\n');
  });
});

describe('NatsProtocolParser', () => {
  test('parses INFO command', () => {
    const parser = new NatsProtocolParser();
    const commands = parser.feed(`INFO {"server_id":"s1","version":"2.0","proto":1,"host":"h","port":4222,"max_payload":1048576}\r\n`);
    expect(commands).toEqual([{ type: 'INFO', data: expect.objectContaining({ server_id: 's1' }) }]);
  });

  test('parses MSG with payload', () => {
    const parser = new NatsProtocolParser();
    const commands = parser.feed('MSG subject 1 5\r\nhello\r\n');
    expect(commands[0].type).toBe('MSG');
    if (commands[0].type === 'MSG') {
      expect(commands[0].message.subject).toBe('subject');
      expect(commands[0].message.payload).toBe('hello');
    }
  });

  test('buffers partial MSG payloads across feeds', () => {
    const parser = new NatsProtocolParser();
    expect(parser.feed('MSG s 1 5\r\nhel')).toEqual([]);
    const commands = parser.feed('lo\r\n');
    expect(commands).toHaveLength(1);
    if (commands[0].type === 'MSG') expect(commands[0].message.payload).toBe('hello');
  });

  test('parses multiple commands in one feed', () => {
    const parser = new NatsProtocolParser();
    const commands = parser.feed('+OK\r\nPING\r\nPONG\r\n');
    expect(commands.map(c => c.type)).toEqual(['+OK', 'PING', 'PONG']);
  });

  test('reset clears buffer state', () => {
    const parser = new NatsProtocolParser();
    parser.feed('MSG s 1 5\r\npar');
    parser.reset();
    expect(parser.feed('tial')).toEqual([]);
  });
});

describe('matchSubject', () => {
  test('exact match', () => {
    expect(matchSubject('a.b.c', 'a.b.c')).toBe(true);
    expect(matchSubject('a.b.c', 'a.b.d')).toBe(false);
  });

  test('* matches exactly one token', () => {
    expect(matchSubject('a.*.c', 'a.b.c')).toBe(true);
    expect(matchSubject('a.*.c', 'a.x.c')).toBe(true);
    expect(matchSubject('a.*.c', 'a.b')).toBe(false);
    expect(matchSubject('a.*.c', 'a.b.c.d')).toBe(false);
  });

  test('> matches one or more trailing tokens', () => {
    expect(matchSubject('a.>', 'a.b.c')).toBe(true);
    expect(matchSubject('a.>', 'a.b')).toBe(true);
    expect(matchSubject('a.>', 'a')).toBe(false);
    expect(matchSubject('>', 'x.y.z')).toBe(true);
  });
});
