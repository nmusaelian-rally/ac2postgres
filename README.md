## AgileCentral data to Postgres example


- uses [pyral](https://pypi.python.org/pypi/pyral) package for AgileCentral (Rally) Rest API
- uses [psycopg2](https://pypi.python.org/pypi/psycopg2/2.6.2) postgres adapter

- creates postgres database tables and columns based on AC Rest API schema endpoint
- sets column type constraints based on allowed values endpoints of respective attributes
- inserts data into the tables

**to run**

switch to postgres user in the terminal and create a database if it does not exist, e.g.
```
sudo su postgres
bash-3.2$ createdb rally
```
in another terminal tab,
to create tables and columns and insert initial data invoke run.py with configuration yml file argument:

```
nmusaelian$ python3.5 run.py config.yml


```
a sample output in the terminal will show the columns and respective types:

```
-CreationDate
---DATE
-ObjectID
---INTEGER
-ScheduleState
---STATE
-FixedInBuild
---STRING
-PlanEstimate
---QUANTITY
-Severity
---RATING
-State
---RATING
-CreationDate
---DATE
-ObjectID
---INTEGER
-ScheduleState
---STATE
-PlanEstimate
---QUANTITY
-c_AliasesOfMilady
---STRING
-c_Musketeer
---STRING
```

optional: to verify the outcome in another terminal tab where you are logged in to the database:

```
bash-3.2$ psql -d rally
psql (9.5.3)
Type "help" for help.

rally=# \d defect
                Table "public.defect"
    Column     |           Type           | Modifiers
---------------+--------------------------+-----------
 creationdate  | timestamp with time zone |
 objectid      | bigint                   | not null
 schedulestate | text                     |
 fixedinbuild  | text                     |
 planestimate  | double precision         |
 severity      | text                     |
 state         | text                     |
Indexes:
    "defect_pkey" PRIMARY KEY, btree (objectid)
Check constraints:
    "defect_schedulestate_check" CHECK (schedulestate = ANY (ARRAY['Defined'::text, 'In-Progress'::text, 'Completed'::text, 'Accepted'::text]))
    "defect_severity_check" CHECK (severity = ANY (ARRAY[''::text, 'Crash/Data Loss'::text, 'Major Problem'::text, 'Minor Problem'::text, 'Cosmetic'::text, 'Test Value '::text]))
    "defect_state_check" CHECK (state = ANY (ARRAY['Submitted'::text, 'Open'::text, 'Fixed'::text, 'Closed'::text]))

rally=# TABLE defect;
        creationdate        |  objectid   | schedulestate | fixedinbuild | planestimate |   severity    |   state
----------------------------+-------------+---------------+--------------+--------------+---------------+-----------
 2016-12-26 12:10:43.704-07 | 83320385428 | Defined       |              |              |               | Submitted
 2016-12-26 12:10:59.458-07 | 83320385700 | Completed     | Foobar       |            2 | Major Problem | Fixed
 2016-12-26 12:11:17.536-07 | 83320386200 | Defined       |              |              |               | Submitted
 2016-12-26 14:00:53.126-07 | 83325295984 | Defined       |              |            4 | Cosmetic      | Submitted
(4 rows)


rally=# TABLE hierarchicalrequirement;
        creationdate        |  objectid   | schedulestate | planestimate | c_aliasesofmilady | c_musketeer
----------------------------+-------------+---------------+--------------+-------------------+-------------
 2016-02-08 09:10:04.591-07 | 50980393212 | Defined       |            3 |                   |
 2016-02-08 09:10:31.358-07 | 50980394041 | Defined       |              |                   |
 2016-02-08 09:20:58.816-07 | 50981048482 | Defined       |           20 |                   |
 2016-12-26 14:00:32.879-07 | 83325293804 | Defined       |              | Anne de Breuil    | Atos
(4 rows)

```