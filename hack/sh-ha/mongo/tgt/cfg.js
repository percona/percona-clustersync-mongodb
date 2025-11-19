rs.initiate({
  _id: 'tgt-cfg',
  configsvr: true,
  members: [
    { _id: 0, host: 'tgt-cfg0:28000', priority: 2 },
    { _id: 1, host: 'tgt-cfg1:28001' },
    { _id: 2, host: 'tgt-cfg2:28002' },
  ],
});
