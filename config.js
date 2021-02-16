'use strict';

module.exports = {
  connection: {
    host: 'cheetah.local',
    port: 8086,
    protocol: 'http',
    database: 'telegraf'
  },
  schema: [],
  oldRetentionPolicyName: 'autogen',
  retentionPolicies: [
    { name: 'two_days', duration: '2d', resolution: '', default: true },
    { name: 'a_week', duration: '7d', resolution: '1m' },
    { name: 'two_months', duration: '61d', resolution: '5m' },
    { name: 'a_year', duration: '366d', resolution: '30m' },
    { name: 'forever', duration: 'INF', resolution: '4h' }
  ],
  aggregates: {
    DEFAULT: 'mean',
    diskio: 'max',
    kernel: { DEFAULT: 'max', entropy_avail: 'mean' },
    net: 'max'
  }
};
