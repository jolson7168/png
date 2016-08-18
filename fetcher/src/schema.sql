
CREATE TABLE public.request_stats  ( 
	datadate           	timestamp NOT NULL,
	url                	varchar(200) NOT NULL,
	apikey             	varchar(50) NOT NULL,
	key1               	varchar(20) NOT NULL,
	key2               	varchar(20) NOT NULL,
	key3               	varchar(20) NOT NULL,
	key4               	varchar(20) NOT NULL,
	requestat          	timestamp NULL,
	requesttime        	numeric(6,3) NULL,
	filesize           	int4 NULL,
	requesthttpresponse	int2 NULL,
	status             	varchar(50) NULL,
	CONSTRAINT fetchstats_pkey PRIMARY KEY(url)
)
DISTSTYLE EVEN
SORTKEY ( datadate )
GO

grant all privileges on table public.request_stats to etl
