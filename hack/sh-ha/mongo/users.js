db.getSiblingDB('admin').createUser({
  user: 'source',
  pwd: 'pass',
  roles: ['backup', 'clusterMonitor', 'readAnyDatabase'],
});

db.getSiblingDB('admin').createUser({
  user: 'target',
  pwd: 'pass',
  roles: ['restore', 'clusterMonitor', 'clusterManager', 'readWriteAnyDatabase'],
});

db.createUser({
  user: "mongodb_exporter",
  pwd: "pass",
  roles: [
    { role: "clusterMonitor", db: "admin" },
    { role: "read", db: "local" },
  ],
})
