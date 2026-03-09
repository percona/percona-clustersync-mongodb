rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "rs00:50000", priority: 2 },
    { _id: 1, host: "rs01:50001" },
    { _id: 2, host: "rs02:50002" },
  ],
});
