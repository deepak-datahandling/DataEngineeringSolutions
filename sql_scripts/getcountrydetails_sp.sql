CREATE PROCEDURE GetCountryDetails
    @RegionName NVARCHAR(100)
AS
BEGIN
    BEGIN TRY
        IF lower(@RegionName) = 'all'
        BEGIN
            SELECT * FROM DWDEV.Weather.CountryDetails;
        END
        ELSE
        BEGIN
            SELECT * FROM DWDEV.Weather.CountryDetails WHERE lower(Region) = lower(@RegionName);        
        END
    END TRY
    BEGIN CATCH
        -- Handle exceptions
        DECLARE @ErrorMessage NVARCHAR(4000)
        DECLARE @ErrorSeverity INT
        DECLARE @ErrorState INT

        -- Get the error details
        SELECT 
            @ErrorMessage = ERROR_MESSAGE(),
            @ErrorSeverity = ERROR_SEVERITY(),
            @ErrorState = ERROR_STATE()

        -- Print the error message
        PRINT 'Error occurred: ' + @ErrorMessage

        -- Optionally, re-raise the error to the calling environment
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState)
    END CATCH
END;