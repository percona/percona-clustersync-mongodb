rs.initiate({
  _id: 'tgt-cfg',
  configsvr: true,
  members: [
    { _id: 0, host: 'tgt-cfg0:28000', priority: 2 },
    // { _id: 1, host: 'cfg1:28101' },
    // { _id: 2, host: 'cfg2:28102' },
  ],
});
