# jdbc Component Change Log 

## 2.3.0 (September 25, 2019)
 
* Add new action `Insert` 
 
## 2.2.0 (August 28, 2019)
 
 * Add support `Configuration properties` for DB connection
 * Enable circleci
 
## 2.1.0 (2019-07-22)

* Add Execute stored procedure action

## 2.0.1 (2019-06-24)

* Fix error logging for generating metadata of `Select` action

## 2.0.0 (2018-09-19)

* Add Select trigger
* Add Get rows polling trigger

* Add Select action
* Add Lookup by primary key action
* Add Upsert by primary key action (for migration)
* Add Delete by primary key action

* Remove CreateOrUpdateRecord action

* Fix issue in postgresql - getDate(null)
* Fix null values as input for select
