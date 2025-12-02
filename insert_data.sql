BULK INSERT incidents
FROM 'D:\Torrid AMS\Torrid Incidents.csv'
WITH (
   FIELDTERMINATOR = ',',
   ROWTERMINATOR = '\n',
   FIRSTROW = 2,
   FORMAT = 'CSV',
   TABLOCK
);

INSERT INTO Users (Username, PasswordHash, Email, Role)
VALUES ('Admin', HASHBYTES('SHA2_256', 'Admin@123'), 'test@gmail.com', 'Admin');