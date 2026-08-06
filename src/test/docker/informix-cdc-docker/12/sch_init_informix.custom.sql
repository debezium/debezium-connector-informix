database sysadmin;
grant dba to root;

execute function admin ('modify chunk extendable', 1);
execute function admin('STORAGEPOOL ADD', '$BASEDIR/data/spaces', 0,0,'256MB',1);
execute function admin('CREATE DBSPACE FROM STORAGEPOOL', 'datadbs', '2 GB');
execute function admin('CREATE TEMPDBSPACE FROM STORAGEPOOL', 'tmpdbspace', '2 GB');
