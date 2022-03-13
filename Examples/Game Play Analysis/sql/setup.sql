/*
Creates table for the exercise and insert values
*/
-- Create Activity table
CREATE TABLE Activity
(
    player_id INT NOT NULL,
    device_id INT NOT NULL,
    event_date [DATE]  NOT NULL,
    games_played INT NOT NULL    
    -- specify more columns here
);
GO

-- Insert into the Activity table
INSERT INTO Activity
   ([player_id],[device_id],[event_date],[games_played])
VALUES
   ( 1, 2, N'2016-03-01', 5),
   ( 1, 2, N'2016-03-02', 6),
   ( 2, 3, N'2017-06-25', 1),
   ( 3, 1, N'2016-03-01', 0),
   ( 3, 4, N'2018-07-03', 5)

GO
