#! /usr/bin/env node

'use strict';

const { InfluxDB, Precision } = require('influx');

const log = console;
const config = {
  connection: {
    host: 'cheetah.local',
    port: 8086,
    protocol: 'http',
    database: 'telegraf'
  },
  schema: [],
  oldRetentionPolicyName: 'autogen',
  retentionPolicies: [
    { name: 'a_hour', duration: '1h', resolution: '', default: true },
    { name: 'a_day', duration: '1d', resolution: '1m' },
    { name: 'a_week', duration: '7d', resolution: '20m' },
    { name: 'forever', duration: 'INF', resolution: '1h' }
  ]
};

async function run() {
  try {
    const influx = new InfluxDB(config.connection);

    const cqs = await await influx.query('SHOW CONTINUOUS QUERIES');
    for (const cq of cqs) {
      log.info(`deleting continuous query "${cq.name}"`);
      await influx.query(`DROP CONTINUOUS QUERY ${cq.name} ON ${config.connection.database}`);
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
      log.info(`transferring data from "${defaultRetentionPolicy.name}".* to "${config.oldRetentionPolicyName}".* retention policy`);
      await influx.query(`SELECT * INTO ${config.oldRetentionPolicyName}.:MEASUREMENT FROM "${defaultRetentionPolicy.name}"./.*/ WHERE time > ${lastTime} GROUP BY *`);
    } else log.info(`retention policy ${defaultRetentionPolicy.name} not exists`);

    for (const rp of rps) {
      // eslint-disable-next-line no-continue
      if (rp.name === config.oldRetentionPolicyName) continue;
      log.info(`deleting retention policy "${rp.name}"`);
      await influx.query(`DROP RETENTION POLICY ${rp.name} ON ${config.connection.database}`);
    }
    log.info(`setting retention policy "${config.oldRetentionPolicyName}" as default`);
    await influx.query(`ALTER RETENTION POLICY ${config.oldRetentionPolicyName} ON ${config.connection.database} DEFAULT`);
  } catch (err) {
    log.error(err);
  }
}

run().catch(err => log.error(err));
