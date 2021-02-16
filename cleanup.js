#! /usr/bin/env node

'use strict';

const { InfluxDB, Precision } = require('influx');
const config = require('./config');
const l = require('./lib');

const log = console;
const dryRun = true; // set false to make changes

async function run() {
  try {
    const influx = new InfluxDB(config.connection);
    l.patch(influx, dryRun);

    const cqs = await await influx.query('SHOW CONTINUOUS QUERIES');
    for (const cq of cqs) {
      log.info(`deleting continuous query "${cq.name}"`);
      await influx.queryRawSoft(l.dropCQQuery(cq.name));
    }

    const rps = await await influx.query('SHOW RETENTION POLICIES');
    const defaultRetentionPolicy = config.retentionPolicies.find(p => p.default);
    if (rps.find(rp => rp.name === defaultRetentionPolicy.name)) {
      const last = await await influx.queryRaw(`SELECT * FROM "${config.oldRetentionPolicyName}"./.*/ ORDER BY time DESC LIMIT 1`, { precision: Precision.Nanoseconds });
      let lastTime = 0;
      for (const s of last.results[0].series) {
        const ti = s.columns.indexOf('time');
        const time = s.values[0][ti];
        if (time > lastTime) lastTime = time;
      }
      if (!lastTime) throw new Error(`cannot get last time of series for "${config.oldRetentionPolicyName}" retention policy`);
      log.info(`transferring data from "${defaultRetentionPolicy.name}".* to "${config.oldRetentionPolicyName}".*`);
      await influx.queryRawSoft(`SELECT * INTO "${config.oldRetentionPolicyName}".:MEASUREMENT FROM "${defaultRetentionPolicy.name}"./.*/ WHERE time > ${lastTime} GROUP BY *`);
    } else log.info(`retention policy "${defaultRetentionPolicy.name}" not exists`);

    for (const rp of rps) {
      // eslint-disable-next-line no-continue
      if (rp.name === config.oldRetentionPolicyName) continue;
      log.info(`deleting retention policy "${rp.name}"`);
      await influx.queryRawSoft(`DROP RETENTION POLICY "${rp.name}" ON "${config.connection.database}"`);
    }
    log.info(`setting retention policy "${config.oldRetentionPolicyName}" as default`);
    await influx.queryRawSoft(`ALTER RETENTION POLICY "${config.oldRetentionPolicyName}" ON "${config.connection.database}" DEFAULT`);
  } catch (err) {
    log.error(err);
  }
}

run().catch(err => log.error(err));
