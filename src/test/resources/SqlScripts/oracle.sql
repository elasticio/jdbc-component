CREATE TABLE StarsWithPK (
        id INTEGER PRIMARY KEY,
        name varchar(255) NOT NULL,
        detected timestamp,
        confirmed date,
        radius float,
        destination int,
        createdat timestamp
);

CREATE TABLE StarsWithoutPK (
        id INTEGER,
        name varchar(255) NOT NULL,
        detected timestamp,
        confirmed date,
        radius float,
        destination int,
        createdat timestamp
);

INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (1, 'Sun', SYSDATE, SYSDATE, 50, 170, SYSDATE);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (2, 'Polaris', SYSDATE - 1, SYSDATE - 1, 150.0, 170, SYSDATE - 1);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (3, 'Algol', SYSDATE - 2, SYSDATE - 2, 350.12, 170, SYSDATE - 2);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (4, 'Sirius', SYSDATE - 3, SYSDATE - 3, 450.65, null, SYSDATE - 3);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (5, 'Antares', SYSDATE - 4, SYSDATE - 4, 550, 170, SYSDATE - 4);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (6, 'Altair', SYSDATE - 5, SYSDATE - 5, null, 170, SYSDATE - 5);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (7, 'Aldebaran', SYSDATE - 6, SYSDATE - 6, 750.877, 170, SYSDATE - 6);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (8, 'Deneb', SYSDATE - 7, null, 850.3, 170, SYSDATE - 7);
         
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (1, 'Sun', SYSDATE, SYSDATE, 50, 170, SYSDATE);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (2, 'Polaris', SYSDATE - 1, SYSDATE - 1, 150.0, 170, SYSDATE - 1);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (3, 'Algol', SYSDATE - 2, SYSDATE - 2, 350.12, 170, SYSDATE - 2);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (4, 'Sirius', SYSDATE - 3, SYSDATE - 3, 450.65, null, SYSDATE - 3);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (5, 'Antares', SYSDATE - 4, SYSDATE - 4, 550, 170, SYSDATE - 4);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (6, 'Altair', SYSDATE - 5, SYSDATE - 5, null, 170, SYSDATE - 5);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (7, 'Aldebaran', SYSDATE - 6, SYSDATE - 6, 750.877, 170, SYSDATE - 6);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
         VALUES (8, 'Deneb', SYSDATE - 7, null, 850.3, 170, SYSDATE - 7);

-- Uncomment if you want to drop created tables
--DROP TABLE StarsWithPK purge;
--DROP TABLE StarsWithoutPK purge;