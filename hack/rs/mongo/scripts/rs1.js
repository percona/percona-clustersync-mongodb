rs.initiate({
  _id: "rs1",
  members: [
    { _id: 0, host: "rs10:50100", priority: 2 },
    { _id: 1, host: "rs11:50101" },
    { _id: 2, host: "rs12:50102" },
  ],
});
