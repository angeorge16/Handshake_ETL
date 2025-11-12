USE DATAMART;
------------------------------ 1. Create master table
IF OBJECT_ID('DATAMART.DBO.HANDSHK_ALUMNI_MASTER') IS NULL
BEGIN
    CREATE TABLE DATAMART.DBO.HANDSHK_ALUMNI_MASTER (
        EDW_PERS_ID BIGINT,
        email_address NVARCHAR(255),
        username NVARCHAR(50),
        auth_identifier NVARCHAR(255),
        card_id NVARCHAR(50),
        first_name NVARCHAR(100),
        last_name NVARCHAR(100),
        middle_name NVARCHAR(100),
        preferred_name NVARCHAR(100),
        school_year_name NVARCHAR(50),
        [primary_education:education_level_name] NVARCHAR(100),
        [primary_education:cumulative_gpa] DECIMAL(10,2),
        [primary_education:department_gpa] DECIMAL(10,2),
        [primary_education:primary_major_name] NVARCHAR(255),
        [primary_education:major_names] NVARCHAR(255),
        [primary_education:minor_names] NVARCHAR(255),
        [primary_education:college_name] NVARCHAR(255),
        [primary_education:start_date] NVARCHAR(50),
        [primary_education:end_date] NVARCHAR(50),
        [primary_education:currently_attending] NVARCHAR(50),
        campus_name NVARCHAR(100),
        opt_cpt_eligible NVARCHAR(50),
        ethnicity NVARCHAR(100),
        gender NVARCHAR(50),
        disabled NVARCHAR(50),
        work_study_eligible NVARCHAR(50),
        system_label_names NVARCHAR(100),
        mobile_number NVARCHAR(50),
        assigned_to_email_address NVARCHAR(255),
        [hometown_location_attributes:name] NVARCHAR(255),
        athlete NVARCHAR(50),
        first_generation NVARCHAR(50),
        veteran NVARCHAR(50),
        eu_gdpr_subject NVARCHAR(50),
        created_dt DATETIME DEFAULT GETDATE(),
		TERM_CD VARCHAR(6),
    );
END
GO  -- important!

