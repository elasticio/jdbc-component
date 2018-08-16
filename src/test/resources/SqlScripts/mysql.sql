CREATE TABLE StarsWithPK (
		id INTEGER,
		name VARCHAR(255) NOT NULL,
		detected DATETIME,
		confirmed DATE,
		radius FLOAT,
		destination INTEGER,
		createdat DATETIME,
		PRIMARY KEY (id)
);

CREATE TABLE StarsWithoutPK (
		id INTEGER,
		name VARCHAR(255) NOT NULL,
		detected DATETIME,
		confirmed DATE,
		radius FLOAT,
		destination INTEGER,
		createdat DATETIME
);

INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (1, 'Sun', NOW(), NOW(), 50, 170, NOW());
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (2, 'Polaris', NOW() - 1, NOW() - 1, 150.0, 170, NOW() - 1);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (3, 'Algol', NOW() - 2, NOW() - 2, 350.12, 170, NOW() - 2);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (4, 'Sirius', NOW() - 3, NOW() - 3, 450.65, null, NOW() - 3);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (5, 'Antares', NOW() - 4, NOW() - 4, 550, 170, NOW() - 4);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (6, 'Altair', NOW() - 5, NOW() - 5, null, 170, NOW() - 5);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (7, 'Aldebaran', NOW() - 6, NOW() - 6, 750.877, 170, NOW() - 6);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (8, 'Deneb', NOW() - 7, null, 850.3, 170, NOW() - 7);
		 
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (1, 'Sun', NOW(), NOW(), 50, 170, NOW());
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (2, 'Polaris', NOW() - 1, NOW() - 1, 150.0, 170, NOW() - 1);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (3, 'Algol', NOW() - 2, NOW() - 2, 350.12, 170, NOW() - 2);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (4, 'Sirius', NOW() - 3, NOW() - 3, 450.65, null, NOW() - 3);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (5, 'Antares', NOW() - 4, NOW() - 4, 550, 170, NOW() - 4);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (6, 'Altair', NOW() - 5, NOW() - 5, null, 170, NOW() - 5);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (7, 'Aldebaran', NOW() - 6, NOW() - 6, 750.877, 170, NOW() - 6);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (8, 'Deneb', NOW() - 7, null, 850.3, 170, NOW() - 7);

		 
/* Uncomment if you want to drop created tables */
/*
DROP TABLE IF EXISTS StarsWithPK;
DROP TABLE IF EXISTS StarsWithoutPK;
*/