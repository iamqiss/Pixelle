# Apache Messenger Node.js Client

Apache Messenger Node.js client written in typescript, it currently only supports tcp & tls transports.

diclaimer: although all messenger commands & basic client/stream are implemented this is still a WIP, provided as is, and has still a long way to go to be considered "battle tested".

note: This lib started as _messenger-bin_ ( [github](https://github.com/T1B0/messenger-bin) / [npm](https://www.npmjs.com/package/messenger-bin)) before migrating under messenger-rs org. package messenger-bin@v1.3.4 is equivalent to @messenger.rs/sdk@v1.0.3 and migrating again under apache messenger monorepo ( [github](https://github.com/apache/messenger/tree/master/foreign/node) and is now published on npmjs as apache-messenger

note: previous works on node.js http client has been moved to [messenger-node-http-client](<https://github.com/messenger-rs/messenger-node-http-client>) (moved on 04 July 2024)

## install

```bash
npm i --save apache-messenger
```

## basic usage

```ts
import { Client } from "apache-messenger";

const credentials = { username: "messenger", password: "messenger" };

const client = new Client({
  transport: "TCP",
  options: { port: 8090, host: "127.0.0.1" },
  credentials,
});

const stats = await client.system.getStats();
```

## use sources

### Install

```bash
npm ci
```

### build

```bash
npm run build
```

### test

note: use env var `MESSENGER_TCP_ADDRESS="host:port"` to set server address for bdd and e2e tests.

#### unit tests

```bash
npm run test:unit
```

#### e2e tests

e2e test expect an messenger-server at tcp://127.0.0.1:8090

```bash
npm run test:e2e
```

#### bdd tests

bdd test expect an messenger-server at tcp://127.0.0.1:8090

```bash
npm run test:bdd
```

#### run all test

`npm run test` runs unit, bdd and e2e tests suite (expect an messenger-server at tcp://127.0.0.1:8090)

### lint

```bash
npm run lint
```
