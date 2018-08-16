CREATE TABLE StarsWithPK (
		id INTEGER,
		name VARCHAR(255) NOT NULL,
		detected TIMESTAMP,
		confirmed DATE,
		radius FLOAT,
		destination INTEGER,
		createdat TIMESTAMP,
		PRIMARY KEY (id)
);

CREATE TABLE StarsWithoutPK (
		id INTEGER,
		name VARCHAR(255) NOT NULL,
		detected TIMESTAMP,
		confirmed DATE,
		radius FLOAT,
		destination INTEGER,
		createdat TIMESTAMP
);

INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (1, 'Sun', NOW(), CURRENT_TIMESTAMP, 50, 170, CURRENT_TIMESTAMP);
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (2, 'Polaris', CURRENT_TIMESTAMP- INTERVAL '1 day', CURRENT_TIMESTAMP- INTERVAL '1 day', 150.0, 170, CURRENT_TIMESTAMP- INTERVAL '1 day');
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (3, 'Algol', CURRENT_TIMESTAMP- INTERVAL '2 days', CURRENT_TIMESTAMP- INTERVAL '2 days', 350.12, 170, CURRENT_TIMESTAMP- INTERVAL '2 days');
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (4, 'Sirius', CURRENT_TIMESTAMP- INTERVAL '3 days', CURRENT_TIMESTAMP- INTERVAL '3 days', 450.65, null, CURRENT_TIMESTAMP- INTERVAL '3 days');
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (5, 'Antares', CURRENT_TIMESTAMP- INTERVAL '4 days', CURRENT_TIMESTAMP- INTERVAL '4 days', 550, 170, CURRENT_TIMESTAMP- INTERVAL '4 days');
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (6, 'Altair', CURRENT_TIMESTAMP- INTERVAL '5 days', CURRENT_TIMESTAMP- INTERVAL '5 days', null, 170, CURRENT_TIMESTAMP- INTERVAL '5 days');
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (7, 'Aldebaran', CURRENT_TIMESTAMP- INTERVAL '6 days', CURRENT_TIMESTAMP- INTERVAL '6 days', 750.877, 170, CURRENT_TIMESTAMP- INTERVAL '6 days');
INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (8, 'Deneb', CURRENT_TIMESTAMP- INTERVAL '7 days', null, 850.3, 170, CURRENT_TIMESTAMP- INTERVAL '7 days');
		 
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (1, 'Sun', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 50, 170, CURRENT_TIMESTAMP);
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (2, 'Polaris', CURRENT_TIMESTAMP- INTERVAL '1 day', CURRENT_TIMESTAMP- INTERVAL '1 day', 150.0, 170, CURRENT_TIMESTAMP- INTERVAL '1 day');
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (3, 'Algol', CURRENT_TIMESTAMP- INTERVAL '2 days', CURRENT_TIMESTAMP- INTERVAL '2 days', 350.12, 170, CURRENT_TIMESTAMP- INTERVAL '2 days');
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (4, 'Sirius', CURRENT_TIMESTAMP - INTERVAL '3 days', CURRENT_TIMESTAMP - INTERVAL '3 days', 450.65, null, CURRENT_TIMESTAMP - INTERVAL '3 days');
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (5, 'Antares', CURRENT_TIMESTAMP - INTERVAL '4 days', CURRENT_TIMESTAMP - INTERVAL '4 days', 550, 170, CURRENT_TIMESTAMP - INTERVAL '4 days');
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (6, 'Altair', CURRENT_TIMESTAMP - INTERVAL '5 days', CURRENT_TIMESTAMP - INTERVAL '5 days', null, 170, CURRENT_TIMESTAMP - INTERVAL '5 days');
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (7, 'Aldebaran', CURRENT_TIMESTAMP - INTERVAL '6 days', CURRENT_TIMESTAMP - INTERVAL '6 days', 750.877, 170, CURRENT_TIMESTAMP - INTERVAL '6 days');
INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
		 VALUES (8, 'Deneb', CURRENT_TIMESTAMP - INTERVAL '7 days', null, 850.3, 170, CURRENT_TIMESTAMP - INTERVAL '7 days');

		 
-- Uncomment if you want to drop created tables 
--DROP TABLE IF EXISTS StarsWithPK;
--DROP TABLE IF EXISTS StarsWithoutPK;
