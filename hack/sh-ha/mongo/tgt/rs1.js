rs.initiate({
  _id: 'rs1',
  members: [
    { _id: 0, host: 'tgt-rs10:40100', priority: 2 },
    { _id: 1, host: 'tgt-rs11:40101' },
    { _id: 2, host: 'tgt-rs12:40102' },
  ],
});
