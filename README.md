## AgileCentral data to Postgres example

**wsapiclient.py**

- Agile Central WSAPI client, uses [requests](https://pypi.python.org/pypi/requests/2.11.1) package

**ac2postgres.py**

- uses wsapiclient.py to talk to AgileCentral and [psycopg2](https://pypi.python.org/pypi/psycopg2/2.6.2) postgres adapter
- creates postgres database tables and columns based on WSAPI schema endpoint
- sets column type constraints based on allowed values endpoints of respective attributes


Outcome of running the script can be verified in the terminal:

```
rally=# \d defect
                Table "public.defect"
    Column     |           Type           | Modifiers 
---------------+--------------------------+-----------
 creationdate  | timestamp with time zone | 
 objectid      | bigint                   | not null
 schedulestate | text                     | 
 planestimate  | numeric                  | 
 state         | text                     | 
Indexes:
    "defect_pkey" PRIMARY KEY, btree (objectid)
Check constraints:
    "defect_schedulestate_check" CHECK (schedulestate = ANY (ARRAY['Not Defined'::text, 'Defined'::text, 'In-Progress'::text, 'Completed'::text, 'Accepted'::text]))
    "defect_state_check" CHECK (state = ANY (ARRAY['Submitted'::text, 'Open'::text, 'Fixed'::text, 'Closed'::text]))
```
**insert.py**

- uses wsapicleint.py to get AgileCentral data (user stories) and populates the hierarchicalrequirements table

**to run**

switch to postgres user in the terminal and create a database if it does not exist, e.g.
```
sudo su postgres
bash-3.2$ createdb rally
```
in the different terminal tab:

```
python3.5 ac2postgres.py
```