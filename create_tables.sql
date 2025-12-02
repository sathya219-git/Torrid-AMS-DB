
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

USE [TorridAMS]
GO
 
/****** Object:  Table [dbo].[UploadHistory]    Script Date: 02-12-2025 15:56:57 ******/
SET ANSI_NULLS ON
GO
 
SET QUOTED_IDENTIFIER ON
GO
 
CREATE TABLE [dbo].[UploadHistory](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[FileName] [nvarchar](255) NOT NULL,
	[FileSize] [bigint] NULL,
	[UploadedDate] [datetime] NOT NULL,
	[FilePath] [nvarchar](1024) NOT NULL,
CONSTRAINT [PK_UploadHistory] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
 
ALTER TABLE [dbo].[UploadHistory] ADD  CONSTRAINT [DF_UploadHistory_UploadedDate]  DEFAULT (getdate()) FOR [UploadedDate]
GO

USE [TorridAMS]
GO
 
/****** Object:  Table [dbo].[StagingIncidents]    Script Date: 02-12-2025 15:57:21 ******/
SET ANSI_NULLS ON
GO
 
SET QUOTED_IDENTIFIER ON
GO
 
CREATE TABLE [dbo].[StagingIncidents](
	[Number] [nvarchar](50) NULL,
	[Opened] [nvarchar](200) NULL,
	[Short_description] [nvarchar](max) NULL,
	[Caller] [nvarchar](200) NULL,
	[Priority] [nvarchar](100) NULL,
	[State] [nvarchar](100) NULL,
	[Category] [nvarchar](200) NULL,
	[Assignment_group] [nvarchar](200) NULL,
	[Assigned_to] [nvarchar](200) NULL,
	[Updated] [nvarchar](200) NULL,
	[Updated_by] [nvarchar](200) NULL,
	[Child_Incidents] [nvarchar](50) NULL,
	[SLA_due] [nvarchar](200) NULL,
	[Severity] [nvarchar](100) NULL,
	[Subcategory] [nvarchar](200) NULL,
	[Resolution_notes] [nvarchar](max) NULL,
	[Resolved] [nvarchar](200) NULL,
	[SLA_Calculation] [nvarchar](200) NULL,
	[Parent_Incident] [nvarchar](200) NULL,
	[Parent] [nvarchar](200) NULL,
	[Task_type] [nvarchar](200) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO