drop keyspace pldb;

create keyspace pldb with replication = { 'class':'SimpleStrategy', 'replication_factor':1 };

use pldb;

create table langs (langname varchar, langdesc varchar, primary key (langname)) with compact storage;
create table langusage (langname varchar, usage counter, totalrating counter, primary key (langname)) with compact storage;
create table projects (projectname varchar, projectdesc varchar, primary key (projectname)) with compact storage;

insert into langs (langname, langdesc) values ('Haskell', 'Lazy non-strict purely functional language');
insert into langs (langname, langdesc) values ('C', 'Imperative language');
insert into langs (langname, langdesc) values ('Java', 'Object-oriented language');

