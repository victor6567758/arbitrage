CREATE TABLE ohcl
(
    id       integer                                NOT NULL CONSTRAINT ohcl_pkey PRIMARY KEY,
    time     timestamp WITH TIME ZONE               NOT NULL,
    symbol   varchar(32)                            NOT NULL,
    open     double precision                       NOT NULL,
    high     double precision                       NOT NULL,
    low      double precision                       NOT NULL,
    close    double precision                       NOT NULL,
    volume   double precision                       NOT NULL
);

ALTER TABLE ohcl
    OWNER TO postgres;
