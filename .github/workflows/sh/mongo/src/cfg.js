rs.initiate({
  _id: 'src-cfg',
  configsvr: true,
  members: [
    { _id: 0, host: 'src-cfg0:27000', priority: 2 },
  ],
});
