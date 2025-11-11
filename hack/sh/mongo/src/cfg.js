rs.initiate({
  _id: 'src-cfg',
  configsvr: true,
  members: [
    { _id: 0, host: 'src-cfg0:27000', priority: 2 },
    // { _id: 1, host: 'cfg1:27001' },
    // { _id: 2, host: 'cfg2:27002' },
  ],
});
