'use strict';

const Diff = require('diff');
const { Precision } = require('influx');
const colors = require('colors/safe');
const parse = require('parse-duration');
const config = require('./config');

const log = console;

function patch(influx, disable) {
  if (disable) {
    influx.querySoft = function query(q) {
      log.info(ts(`${colors.cyan(q)}\n`));
    };
    influx.queryRawSoft = function queryRaw(q) {
      log.info(ts(`${colors.cyan(q)}\n`));
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
  const meas = config.schema.find(s => s.measurement === measurement);
  if (!meas) return '';
  const whereClause = retentionPolicy.duration.toUpperCase() === 'INF' ? '' : ` WHERE time >= now() - ${retentionPolicy.duration}`;
  const fieldsClause = meas.fields.map(f => `${agg(measurement, f)}("${f}") AS "${f}"`).join(', ');
  return `SELECT ${fieldsClause} INTO "${retentionPolicy.name}"."${measurement}" FROM "${config.oldRetentionPolicyName}"."${measurement}"${whereClause} GROUP BY time(${retentionPolicy.resolution}), *`;
}

function createCQQuery(retentionPolicy, defaultRetentionPolicy, measurement) {
  if (!retentionPolicy.resolution) return '';
  const meas = config.schema.find(s => s.measurement === measurement);
  if (!meas) return '';
  const fieldsClause = meas.fields.map(f => `${agg(measurement, f)}("${f}") AS "${f}"`).join(', ');
  return `CREATE CONTINUOUS QUERY "cq_${measurement}_${retentionPolicy.resolution}" ON "${config.connection.database}" BEGIN SELECT ${fieldsClause} INTO "${config.connection.database}"."${retentionPolicy.name}"."${measurement}" FROM "${config.connection.database}"."${defaultRetentionPolicy.name}"."${measurement}" GROUP BY time(${retentionPolicy.resolution}), * END`;
}

function dropCQQuery(name) {
  return `DROP CONTINUOUS QUERY "${name}" ON "${config.connection.database}"`;
}

async function writeGrafanaRPData(influx, dryRun) {
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

function agg(measure, field) {
  let result = 'mean';
  if (isString(config.aggregates.DEFAULT)) result = config.aggregates.DEFAULT;
  const measureAgg = config.aggregates[measure];
  if (!measureAgg) return result;
  if (isString(measureAgg)) return measureAgg;
  if (isString(measureAgg.DEFAULT)) result = measureAgg.DEFAULT;
  if (isString(measureAgg[field])) result = measureAgg[field];
  return result;
}

function isString(value) {
  return ![undefined, null].includes(value) && value.constructor === String && value;
}

function toNanoSec(s) {
  if (['INF', '0S'].includes(s.toUpperCase())) return '9223372036854775806'; // maximum time in nanoseconds
  return parse(s, 'ns');
}

function isEqualQueries(q1, q2) {
  const diff = Diff.diffChars(q1, q2, { ignoreCase: true });
  const diffStr = diff
    .filter(part => part.added || part.removed)
    .map(part => part.value)
    .join('');
  return /^[\s"]*$/.test(diffStr);
}

function ms(timeout) {
  return new Promise(resolve => (typeof timeout === 'number' ? setTimeout(resolve, timeout) : setImmediate(resolve)));
}

function ts(text) {
  const nowDate = new Date();
  return [nowDate.toTimeString().split(' ')[0], text].join(' ');
}

module.exports = { patch, updateSchema, createRPQuery, createTransferToDefRPQuery, createDownsampleQuery, createCQQuery, dropCQQuery, writeGrafanaRPData, toNanoSec, isEqualQueries, ms, ts, agg };
