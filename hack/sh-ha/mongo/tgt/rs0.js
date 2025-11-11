rs.initiate({
  _id: 'rs0',
  members: [
    { _id: 0, host: 'tgt-rs00:40000', priority: 2 },
    { _id: 1, host: 'tgt-rs01:40001' },
    { _id: 2, host: 'tgt-rs02:40002' },
  ],
});
