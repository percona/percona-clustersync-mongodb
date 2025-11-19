db.getSiblingDB('admin').createUser({
    user: 'adm',
    pwd: 'pass',
    roles: ['root'],
});
