rs.initiate({
  _id: 'tgt-cfg',
  configsvr: true,
  members: [
    { _id: 0, host: 'tgt-cfg0:28000', priority: 2 },
  ],
});
