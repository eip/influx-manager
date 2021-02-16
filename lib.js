'use strict';

const { Precision } = require('influx');
const config = require('./config');

const log = console;

function patch(influx, disable) {
  if (disable) {
    influx.querySoft = function query(q) {
      log.info(`\x1b[0;36m${q}\x1b[0m\n`);
    };
    influx.queryRawSoft = function queryRaw(q) {
      log.info(`\x1b[0;36m${q}\x1b[0m\n`);
    };
    return;
  }
  influx.querySoft = influx.query;
  influx.queryRawSoft = influx.queryRaw;
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
  const whereClause = retentionPolicy.duration.toUpperCase() === 'INF' ? '' : ` WHERE time >= now() - ${retentionPolicy.duration}`;
  const fieldsClause = ms.fields.map(f => `mean("${f}") AS "${f}"`).join(', ');
  return `SELECT ${fieldsClause} INTO "${retentionPolicy.name}"."${measurement}" FROM "${config.oldRetentionPolicyName}"."${measurement}"${whereClause} GROUP BY time(${retentionPolicy.resolution}), *`;
}

function createCQQuery(retentionPolicy, defaultRetentionPolicy, measurement) {
  if (!retentionPolicy.resolution) return '';
  const ms = config.schema.find(s => s.measurement === measurement);
  if (!ms) return '';
  const fieldsClause = ms.fields.map(f => `mean("${f}") AS "${f}"`).join(', ');
  return `CREATE CONTINUOUS QUERY "cq_${measurement}_${retentionPolicy.resolution}" ON "${config.connection.database}" BEGIN SELECT ${fieldsClause} INTO "${config.connection.database}"."${retentionPolicy.name}"."${measurement}" FROM "${config.connection.database}"."${defaultRetentionPolicy.name}"."${measurement}" GROUP BY time(${retentionPolicy.resolution}), * END`;
}

async function insertGrafanaRPData(influx, dryRun) {
  const targetRP = config.retentionPolicies.find(rp => rp.duration.toUpperCase() === 'INF');
  if (!targetRP) return '';
  const points = [];
  for (const rp of config.retentionPolicies) {
    points.push({ measurement: 'grafana_rp', fields: { rp: rp.name }, timestamp: toNanoSec(rp.duration) });
  }
  if (!dryRun) {
    return influx.writePoints(points, { retentionPolicy: targetRP.name, precision: Precision.Nanoseconds });
  }
  log.info(`\x1b[0;36m${points.map(p => `POINT: ${JSON.stringify(p)}`).join('\n')}\x1b[0m\n`);
  return null;
}

function toNanoSec(s) {
  if (s.toUpperCase() === 'INF') return '9223372036854775806';
  const unit = s.slice(-1).toLowerCase();
  const val = parseInt(s.slice(0, -1), 10);
  switch (unit) {
    case 'h':
      return `${(val * 3600) | 0}000000000`;
    case 'd':
      return `${(val * 24 * 3600) | 0}000000000`;
    default:
      throw new Error(`unknown unit: ${unit}`);
  }
}

function stripQuotes(s) {
  return s.replace(/"/g, '');
}

module.exports = { patch, updateSchema, createRPQuery, createTransferToDefRPQuery, createDownsampleQuery, createCQQuery, insertGrafanaRPData, stripQuotes };
