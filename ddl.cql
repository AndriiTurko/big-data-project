CREATE KEYSPACE wikipedia-project WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE wikipedia-project;

CREATE TABLE category_a
(
    id int,
    domain text,
    creation_time timestamp,
    user_id text,
    user_name text,
    page text,
    is_bot boolean,
    PRIMARY KEY ((domain), time, user_id)
);

