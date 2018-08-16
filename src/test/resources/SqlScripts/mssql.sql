DECLARE @CreateOrDrop INT = 0  -- 0 - create
                               -- 1 - drop
                            
IF @CreateOrDrop = 0
BEGIN
        CREATE TABLE StarsWithPK (
                id INTEGER PRIMARY KEY,
                name varchar(255) NOT NULL,
                detected datetime,
                confirmed date,
                radius float,
                destination int,
                createdat datetime
        )
        
        CREATE TABLE StarsWithoutPK (
                id INTEGER,
                name varchar(255) NOT NULL,
                detected datetime,
                confirmed date,
                radius float,
                destination int,
                createdat datetime
        )
        
        INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (1, 'Sun', GETDATE(), GETDATE(), 50, 170, GETDATE())
        INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (2, 'Polaris', GETDATE() - 1, GETDATE() - 1, 150.0, 170, GETDATE() - 1)
        INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (3, 'Algol', GETDATE() - 2, GETDATE() - 2, 350.12, 170, GETDATE() - 2)
        INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (4, 'Sirius', GETDATE() - 3, GETDATE() - 3, 450.65, null, GETDATE() - 3)
        INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (5, 'Antares', GETDATE() - 4, GETDATE() - 4, 550, 170, GETDATE() - 4)
        INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (6, 'Altair', GETDATE() - 5, GETDATE() - 5, null, 170, GETDATE() - 5)
        INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (7, 'Aldebaran', GETDATE() - 6, GETDATE() - 6, 750.877, 170, GETDATE() - 6)
        INSERT INTO StarsWithPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (8, 'Deneb', GETDATE() - 7, null, 850.3, 170, GETDATE() - 7)
                 
        INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (1, 'Sun', GETDATE(), GETDATE(), 50, 170, GETDATE())
        INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (2, 'Polaris', GETDATE() - 1, GETDATE() - 1, 150.0, 170, GETDATE() - 1)
        INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (3, 'Algol', GETDATE() - 2, GETDATE() - 2, 350.12, 170, GETDATE() - 2)
        INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (4, 'Sirius', GETDATE() - 3, GETDATE() - 3, 450.65, null, GETDATE() - 3)
        INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (5, 'Antares', GETDATE() - 4, GETDATE() - 4, 550, 170, GETDATE() - 4)
        INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (6, 'Altair', GETDATE() - 5, GETDATE() - 5, null, 170, GETDATE() - 5)
        INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (7, 'Aldebaran', GETDATE() - 6, GETDATE() - 6, 750.877, 170, GETDATE() - 6)
        INSERT INTO StarsWithoutPK (id, name, detected, confirmed, radius, destination, createdat) 
                 VALUES (8, 'Deneb', GETDATE() - 7, null, 850.3, 170, GETDATE() - 7)
END
ELSE
BEGIN
        IF OBJECT_ID('StarsWithPK', 'U') IS NOT NULL DROP TABLE StarsWithPK
        IF OBJECT_ID('StarsWithoutPK', 'U') IS NOT NULL DROP TABLE StarsWithoutPK
END
GO