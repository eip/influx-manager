#! /usr/bin/env node

'use strict';

const { InfluxDB } = require('influx');
const config = require('./config');

const log = console;

async function run() {
  try {
    const influx = new InfluxDB(config.connection);

    const rps = await await influx.query('SHOW RETENTION POLICIES');
    if (rps.length > 1) {
      log.info(`retention policies ${rps.map(rp => `"${rp.name}"`).join(', ')} already exist`);
      return;
    }
    await updateSchema(influx);
    const defaultRetentionPolicy = config.retentionPolicies.find(p => p.default);
    for (const rp of config.retentionPolicies) {
      log.info(`creating retention policy "${rp.name}"`);
      await influx.queryRaw(logQuery(createRPQuery(rp)));
    }
    log.info(`transferring data from "${config.oldRetentionPolicyName}".* to "${defaultRetentionPolicy.name}".* retention policy`);
    await influx.queryRaw(logQuery(createTransferToDefRPQuery(defaultRetentionPolicy)));
    for (const ms of config.schema) {
      for (const rp of config.retentionPolicies.filter(p => !p.default)) {
        log.info(`downsampling data from "${config.oldRetentionPolicyName}"."${ms.measurement}" to "${rp.name}"."${ms.measurement}" retention policy`);
        await influx.queryRaw(logQuery(createDownsampleQuery(rp, ms.measurement)));
      }
    }
    for (const ms of config.schema) {
      for (const rp of config.retentionPolicies.filter(p => !p.default)) {
        log.info(`creating continuous query for "${rp.name}"."${ms.measurement}"`);
        await influx.queryRaw(logQuery(createCQQuery(rp, defaultRetentionPolicy, ms.measurement)));
      }
    }
  } catch (err) {
    log.error(err);
  }
}

async function updateSchema(influx) {
  const res = await influx.query('SHOW FIELD KEYS');
  for (const mr of res.groupRows) {
    const measurement = mr.name;
    const fields = mr.rows.map(r => r.fieldKey);
    config.schema.push({ measurement, fields });
  }
}

function createRPQuery(retentionPolicy) {
  return `CREATE RETENTION POLICY "${retentionPolicy.name}" ON "${config.connection.database}" DURATION ${retentionPolicy.duration} REPLICATION 1${retentionPolicy.default ? ' DEFAULT' : ''}`;
}

function createTransferToDefRPQuery(defaultRetentionPolicy) {
  return `SELECT * INTO ${defaultRetentionPolicy.name}.:MEASUREMENT FROM ${config.oldRetentionPolicyName}./.*/ WHERE time >= now() - ${defaultRetentionPolicy.duration} GROUP BY *`;
}

function createDownsampleQuery(retentionPolicy, measurement) {
  if (!retentionPolicy.resolution) return '';
  const ms = config.schema.find(s => s.measurement === measurement);
  if (!ms) return '';
  const whereClause = retentionPolicy.duration === 'INF' ? '' : ` WHERE time >= now() - ${retentionPolicy.duration}`;
  const fieldsClause = ms.fields.map(f => `mean("${f}") AS "${f}"`).join(', ');
  return `SELECT ${fieldsClause} INTO "${retentionPolicy.name}"."${measurement}" FROM "${config.oldRetentionPolicyName}"."${measurement}"${whereClause} GROUP BY time(${retentionPolicy.resolution}), *`;
}

function createCQQuery(retentionPolicy, defaultRetentionPolicy, measurement) {
  if (!retentionPolicy.resolution) return '';
  const ms = config.schema.find(s => s.measurement === measurement);
  if (!ms) return '';
  const fieldsClause = ms.fields.map(f => `mean("${f}") AS "${f}"`).join(', ');
  return `CREATE CONTINUOUS QUERY "cq_${measurement}_${retentionPolicy.resolution}" ON "${config.connection.database}" BEGIN SELECT ${fieldsClause} INTO "${retentionPolicy.name}"."${measurement}" FROM "${defaultRetentionPolicy.name}"."${measurement}" GROUP BY time(${retentionPolicy.resolution}), * END`;
}

function logQuery(query) {
  if (config.debug) log.debug(query);
  return query;
}

run().catch(err => log.error(err));
