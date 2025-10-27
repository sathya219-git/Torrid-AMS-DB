
CREATE DATABASE Torrid_AMS;
GO

USE Torrid_AMS;
GO

CREATE TABLE incidents(
   Number VARCHAR(20) PRIMARY KEY,
   Opened DATETIME,
   Short_description TEXT,
   Caller VARCHAR(100),
   Priority VARCHAR(50),
   State VARCHAR(50),
   Category VARCHAR(100),
   Assignment_group VARCHAR(100),
   Assigned_to VARCHAR(100),
   Updated DATETIME,
   Updated_by VARCHAR(100),
   Child_Incidents INT,
   SLA_due VARCHAR(20) NULL,
   Severity VARCHAR(50),
   Subcategory VARCHAR(100),
   Resolution_notes TEXT,
   Resolved DATETIME,
   SLA_Calculation VARCHAR(20) NULL,
   Parent_Incident VARCHAR(20) NULL,
   Parent VARCHAR(100),
   Task_type VARCHAR(50)
);

CREATE TABLE Users (
    UserID INT IDENTITY(1,1) PRIMARY KEY,       -- Unique user ID
    Username NVARCHAR(100) NOT NULL UNIQUE,     -- Login username
    PasswordHash VARBINARY(64) NOT NULL,        -- Hashed password (SHA2_256)
    Email NVARCHAR(255) NULL,                   -- Optional user email
    Role NVARCHAR(50) NULL,                     -- Role (Admin, User, etc.)
    CreatedDate DATETIME DEFAULT GETDATE(),     -- When account was created
    IsActive BIT DEFAULT 1                      -- 1 = Active, 0 = Disabled
);