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

    // const rps = await await influx.query('SHOW RETENTION POLICIES');
    // if (rps.length > 1) {
    //   log.info(`retention policies ${rps.map(rp => `"${rp.name}"`).join(', ')} already exist`);
    //   return;
    // }
    await l.updateSchema(influx);
    const defaultRetentionPolicy = config.retentionPolicies.find(p => p.default);
    // for (const rp of config.retentionPolicies) {
    //   log.info(`creating retention policy "${rp.name}"`);
    //   await influx.queryRaw(logQuery(createRPQuery(rp)));
    // }
    // log.info(`transferring data from "${config.oldRetentionPolicyName}".* to "${defaultRetentionPolicy.name}".* retention policy`);
    // await influx.queryRaw(logQuery(createTransferToDefRPQuery(defaultRetentionPolicy)));
    // for (const ms of config.schema) {
    //   for (const rp of config.retentionPolicies.filter(p => !p.default)) {
    //     log.info(`downsampling data from "${config.oldRetentionPolicyName}"."${ms.measurement}" to "${rp.name}"."${ms.measurement}" retention policy`);
    //     await influx.queryRaw(logQuery(createDownsampleQuery(rp, ms.measurement)));
    //   }
    // }
    const wantCQs = [];
    for (const ms of config.schema) {
      for (const rp of config.retentionPolicies.filter(p => !p.default)) {
        wantCQs.push({ name: `cq_${ms.measurement}_${rp.resolution}`, query: l.createCQQuery(rp, defaultRetentionPolicy, ms.measurement) });
      }
    }
    const existCQs = [...(await await influx.query('SHOW CONTINUOUS QUERIES'))];
    const deleteCQs = existCQs.filter(ecq => !wantCQs.find(wcq => wcq.name === ecq.name));
    const addCQs = wantCQs.filter(wcq => !existCQs.find(ecq => ecq.name === wcq.name));
    for (const ecq of existCQs) {
      const wcq = wantCQs.find(cq => cq.name === ecq.name);
      if (l.isEqualQueries(ecq.query, wcq.query)) break;
      deleteCQs.push(ecq);
      addCQs.push(wcq);
    }
    for (const dcq of deleteCQs) {
      log.info(`deleting continuous query "${dcq.name}"`);
      await influx.queryRawSoft(l.dropCQQuery(dcq.name));
    }
    for (const acq of addCQs) {
      log.info(`creating continuous query "${acq.name}"`);
      await influx.queryRawSoft(acq.query);
    }
  } catch (err) {
    log.error(err);
  }
}

run().catch(err => log.error(err));
