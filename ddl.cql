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
    PRIMARY KEY ((domain), creation_time, user_id)
);

CREATE TABLE page_info
(
    page_id int,
    page text,
    PRIMARY KEY ((page_id))
);