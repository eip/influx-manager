#! /usr/bin/env node

'use strict';

const { InfluxDB } = require('influx');
const config = require('./config');
const l = require('./lib');

const log = console;
const dryRun = true; // set false to make changes

async function run() {
  try {
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
      log.info(`creating retention policy "${rp.name}"`);
      await influx.queryRawSoft(l.createRPQuery(rp));
    }
    log.info(`transferring data from "${config.oldRetentionPolicyName}".* to "${defaultRetentionPolicy.name}".* retention policy`);
    await influx.queryRawSoft(l.createTransferToDefRPQuery(defaultRetentionPolicy));
    for (const ms of config.schema) {
      for (const rp of config.retentionPolicies.filter(p => !p.default)) {
        log.info(`downsampling data from "${config.oldRetentionPolicyName}"."${ms.measurement}" to "${rp.name}"."${ms.measurement}" retention policy`);
        await influx.queryRawSoft(l.createDownsampleQuery(rp, ms.measurement));
      }
    }
    for (const ms of config.schema) {
      for (const rp of config.retentionPolicies.filter(p => !p.default)) {
        log.info(`creating continuous query for "${rp.name}"."${ms.measurement}"`);
        await influx.queryRawSoft(l.createCQQuery(rp, defaultRetentionPolicy, ms.measurement));
      }
    }
  } catch (err) {
    log.error(err);
  }
}

run().catch(err => log.error(err));
