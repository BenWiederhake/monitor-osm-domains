# monosmdom_server

This is the server that constantly runs, it's activity is the namesake of this
project: It monitors OSM domains, and saves the results. These can then later
be used to query for problems and check which actions should be taken, if any.

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Structure](#structure)
- [Performance](#performance)
- [TODOs](#todos)
- [NOTDOs](#notdos)
- [Contribute](#contribute)

## Install

- Install required system packages. For example, on Debian:
  ```
  $ apt install python3 postgresql psql
  ```
- Get postgres up and running. You might find [these instructions](https://www.postgresql.org/docs/14/client-authentication.html) useful.
  Create a database of some name, for example `monosmdom`.
  You can check your progress with the `psql` tool.
  ```
  > create role ACTUAL_LOCAL_USERNAME with login createdb encrypted password 'SECRETPASSWORDFORTHISDATABASE';`
  > create database monosmdom owner ACTUAL_LOCAL_USERNAME;
  > alter database monosmdom set timezone = 'UTC';
  ```
- Set up a local virtualenv. For example, with the `venv` module:
  ```
  $ python3 -m venv .venv
  $ source ./venv/bin/activate
  ```
- Install required packages:
  ```
  $ pip install -r requirements.txt
  ```
  If you have problems later on, try `pip install --upgrade psycopg[binary]`
- Copy the file `secret_config_template.py` to `secret_config.py`, and fill in the connection details from above.
  At this point, you can check whether Django can connect with Postgresql:
  ```
  $ ./manage.py dbshell
  ```
- Populate the database and server files:
  ```
  $ ./manage.py migrate
  $ ./manage.py createsuperuser
  $ ./manage.py collectstatic
  ```

Updates work similarly:
```
$ git pull
$ ./manage.py migrate
# systemctl restart apache
```

## Usage

Once it's installed, you should be able to run it however you wish (e.g. `./manage.py runserver 127.0.0.1:8080` for a quick-and-dirty local setup), and it should go from there.

Before actually going into public production, you should take a close look at the [deployment checklist](https://docs.djangoproject.com/en/4.2/howto/deployment/checklist/), including in particular:
    $ ./manage.py check --deploy

I'll probably follow this setup very closely: https://lab.uberspace.de/guide_django/

## Structure

I'll try to adhere to the Django philosophy this time, i.e.: The "project" is just a collection of "apps", and all functionality is written in separate "apps" that could, in theory, be switched on and off.

- storage: data models, data migrations, intake script, export
- webui: auth models, auth data, inspection
- crawler: actual activity
- (FUTURE) osmbot: submit changesets to OSM

## Performance

Since there is only one user (me), and very little to do (rarely take in a huge amount of new data; query websites every now and then; rarely respond to the user viewing the state of things), performance doesn't really matter.

## TODOs

* Everything
  * Determine the questions that are asked, and what the data model needs to look like
  * Import the data
  * Start automating the first queries

## NOTDOs

See project page.

## Contribute

Feel free to dive in! [Open an issue](https://github.com/BenWiederhake/monitor-osm-domains/issues/new) or submit PRs.
