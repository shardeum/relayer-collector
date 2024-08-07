# Collector

The Data Distribution System is designed to efficiently handle the flow of data within the ecosystem. This system involves various components such as Collectors, Archivers, Validators, Distributors, and more. The system revolves around the Collector, which saves incoming data chunks from Archivers or Distributors into log files and an SQLite database. Active Validators generate data passed on to Archivers, which in turn distribute it to various services like Explorers, JSON-RPC servers, and wallets for end-user access.

## Development

1. Clone the repository and switch to `dev` branch

```bash
git clone https://gitlab.com/shardus/relayer/collector.git
git switch dev
```

2. Configure the `config.json` file according to your requirements

- Set the **ip**, **port** and **publicKey** of the distributor service you're connecting to in the `distributorInfo` object.
- Set the public and secret keys of the collector service in the `collectorInfo` object.

3. Install dependencies

```bash
npm install && npm run prepare
```

4. Run the collector service
   
```bash
npm run collector
```

## Working

### Collector Service

The Collector service connects to a distributor service as configured in `config.json`. It can be started with the following command:

```bash
npm run collector
```

Here's what the Collector does:

**Data Syncing:** Initiates a data syncing process in which it queries all the historical data (Accounts, Transactions, Receipts etc) from the distributor and saves it to its own SQLite DB.
**Real-time Data Subscription:** Simultaneously, it subscribes to real-time data from the distributor and writes it to a log file (under **`/data-logs`**) and the SQLite DB.

### Server

This API Server is used to expose the data stored in the SQLite DB to the RPC Server, if you intend to point an RPC to the collector data. The server can be started using:

```bash
npm run server
```

### Log Server

The **`log_server`** hosts an endpoint named `/evm_log_subscription`, specifically designed for hosting on-chain event logs like `eth_getLogs`. This feature further enhances the capabilities and accessibility of the system.

The log server can be started using:

```bash
npm run log_server
```

### Health Check

- GET `/is-alive` this endpoint returns 200 if the server is running.
- GET `/is-healthy` currently the same as `/is-alive` but will be expanded.

## Contributing

Contributions are highly encouraged! We welcome everyone to participate in our codebases, issue trackers, and any other form of communication. However, we expect all contributors to adhere to our [code of conduct](./CODE_OF_CONDUCT.md) to ensure a positive and collaborative environment for all involved in the project.
