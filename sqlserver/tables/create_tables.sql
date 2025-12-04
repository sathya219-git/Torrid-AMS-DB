
CREATE DATABASE Torrid_AMS;
GO

USE Torrid_AMS;
GO

CREATE TABLE [dbo].[Incidents](
	[Number] [nvarchar](50) NOT NULL,
	[Opened] [datetime] NULL,
	[Short_description] [nvarchar](max) NULL,
	[Caller] [nvarchar](100) NULL,
	[Priority] [nvarchar](50) NULL,
	[State] [nvarchar](50) NULL,
	[Category] [nvarchar](100) NULL,
	[Assignment_group] [nvarchar](100) NULL,
	[Assigned_to] [nvarchar](100) NULL,
	[Updated] [datetime] NULL,
	[Updated_by] [nvarchar](100) NULL,
	[Child_Incidents] [nvarchar](max) NULL,
	[SLA_due] [nvarchar](50) NULL,
	[Severity] [nvarchar](50) NULL,
	[Subcategory] [nvarchar](100) NULL,
	[Resolution_notes] [nvarchar](max) NULL,
	[Resolved] [datetime] NULL,
	[SLA_Calculation] [nvarchar](50) NULL,
	[Parent_Incident] [nvarchar](50) NULL,
	[Parent] [nvarchar](50) NULL,
	[Task_type] [nvarchar](100) NULL,
PRIMARY KEY CLUSTERED 
(
	[Number] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

CREATE TABLE Users (
    UserID INT IDENTITY(1,1) PRIMARY KEY,       -- Unique user ID
    Username NVARCHAR(100) NOT NULL UNIQUE,     -- Login username
    PasswordHash VARBINARY(64) NOT NULL,        -- Hashed password (SHA2_256)
    Email NVARCHAR(255) NULL,                   -- Optional user email
    Role NVARCHAR(50) NULL,                     -- Role (Admin, User, etc.)
    CreatedDate DATETIME DEFAULT GETDATE(),     -- When account was created
    IsActive BIT DEFAULT 1                      -- 1 = Active, 0 = Disabled
);