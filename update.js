#! /usr/bin/env node
/* eslint-disable no-continue */

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
    const messages = [];
    for (const rp of rps) {
      const wantRP = config.retentionPolicies.find(wrp => wrp.name === rp.name);
      if (!wantRP) {
        if (rp.name === config.oldRetentionPolicyName) continue;
        messages.push(`redundant retention policy "${rp.name}"`);
        continue;
      }
      if (l.toNanoSec(rp.duration) !== l.toNanoSec(wantRP.duration)) {
        messages.push(`retention policy "${rp.name}": duration ${rp.duration}, want ${wantRP.duration}`);
        continue;
      }
      if (!rp.default !== !wantRP.default) {
        messages.push(`retention policy "${rp.name}": default ${rp.default}, want ${wantRP.default}`);
        continue;
      }
    }
    if (messages.length) {
      log.info(messages.join('\n'));
      log.info('need to update retention policies');
      return;
    }
    await l.updateSchema(influx);
    const defaultRetentionPolicy = config.retentionPolicies.find(p => p.default);
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
      if (l.isEqualQueries(ecq.query, wcq.query)) continue;
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
