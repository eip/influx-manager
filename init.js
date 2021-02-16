#! /usr/bin/env node

'use strict';

const { InfluxDB } = require('influx');
const config = require('./config');
const l = require('./lib');

const log = console;
const dryRun = true; // set false to make changes

async function run() {
  const influx = new InfluxDB(config.connection);
  l.patch(influx, dryRun);

  const rps = await await influx.query('SHOW RETENTION POLICIES');
  if (rps.length > 1) {
    log.info(`retention policies ${rps.map(rp => `"${rp.name}"`).join(', ')} already exist`);
    return;
  }
  await l.updateSchema(influx);

  const defaultRetentionPolicy = config.retentionPolicies.find(p => p.default);
  for (const rp of config.retentionPolicies) {
    log.info(l.ts(`creating retention policy "${rp.name}"`));
    await influx.queryRawSoft(l.createRPQuery(rp));
  }

  for (const ms of config.schema) {
    for (const rp of config.retentionPolicies.filter(p => !p.default)) {
      log.info(l.ts(`creating continuous query for "${rp.name}"."${ms.measurement}"`));
      await influx.queryRawSoft(l.createCQQuery(rp, defaultRetentionPolicy, ms.measurement));
    }
  }

  log.info(l.ts(`transferring data from "${config.oldRetentionPolicyName}".* to "${defaultRetentionPolicy.name}".*`));
  await influx.queryRawSoft(l.createTransferToDefRPQuery(defaultRetentionPolicy));

  for (const rp of config.retentionPolicies.filter(p => !p.default)) {
    for (const ms of config.schema) {
      log.info(l.ts(`downsampling data from "${config.oldRetentionPolicyName}"."${ms.measurement}" to "${rp.name}"."${ms.measurement}"`));
      const startTime = Date.now();
      await influx.queryRawSoft(l.createDownsampleQuery(rp, ms.measurement));
      await l.ms((Date.now() - startTime) * 1.5);
    }
  }

  log.info(l.ts('writing retention policies data for grafana'));
  await l.writeGrafanaRPData(influx, dryRun);
}

run().catch(err => log.error(err));
