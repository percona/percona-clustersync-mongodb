let conf = rs.config()

for (let member of conf.members) {
  delete member.priority
}

rs.reconfig(conf);
