-- DDL

-- Create Schema
create schema ygb19mzu_library_cw;

-- Set search path to schema
set search_path to ygb19mzu_library_cw,public;

--============================== DDL ============================================

CREATE TABLE event (
    ecode        CHAR(4) primary key,
    edesc        VARCHAR(20) not null,
    elocation    VARCHAR(20) not null,
    edate        DATE Check (edate >= '2024-07-01' and edate <= '2024-07-31'),
    etime        TIME Check (etime >= '09:00:00'),
    emax         SMALLINT check(emax >= 1 and emax <= 1000)
);

CREATE TABLE spectator (
    sno          INTEGER primary key,
    sname        VARCHAR(20) not null,
    semail       VARCHAR(20) not null
);

CREATE TABLE ticket (
    tno          INTEGER primary key,
    ecode        CHAR(4),
    sno          INTEGER,
	
	foreign key (ecode) references event(ecode),
	foreign key (sno) references spectator(sno)
);

CREATE TABLE cancel (
    tno          INTEGER,
    ecode        CHAR(4),
    sno          INTEGER,
    cdate        TIMESTAMP,
    cuser        VARCHAR(128), 
	
	primary key(tno, ecode),
	foreign key(tno) references ticket(tno) on delete cascade on update cascade,
	foreign key(ecode) references event(ecode) on delete cascade on update cascade,
	foreign key(sno) references spectator(sno)on delete cascade on update cascade
);
--============================== Functions ============================================

-- Create a function to check if a spectator has a ticket
CREATE OR REPLACE FUNCTION check_spectator_tickets()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM ticket
        WHERE sno = OLD.sno
    ) THEN
        RAISE EXCEPTION 'Spectator has tickets and cannot be deleted.';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Create a function to check if an event has a ticket still active
CREATE OR REPLACE FUNCTION before_delete_event()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if there are associated tickets
    IF EXISTS (SELECT 1 FROM ticket WHERE ecode = OLD.ecode) THEN
        RAISE EXCEPTION 'Cannot delete event with associated tickets';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- Create a function to check if a spectator already has tickets for an existing event
CREATE OR REPLACE FUNCTION check_existing_ticket()
RETURNS TRIGGER AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM ticket
    WHERE sno = NEW.sno
      AND ecode = NEW.ecode
  ) THEN
    RAISE EXCEPTION 'Spectator already has a ticket for the event';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- Check if a ticket is valid
CREATE OR REPLACE FUNCTION check_ticket_status(ticket_no INTEGER)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM ticket
        WHERE tno = ticket_no
    );
END;
$$ LANGUAGE plpgsql;




--============================== Triggers ============================================

-- Create a trigger to call the check_spectator_tickets() function before deleting a spectator
CREATE TRIGGER before_delete_spectator
BEFORE DELETE ON spectator
FOR EACH ROW
EXECUTE FUNCTION check_spectator_tickets();

-- Create a trigger to call the before_delete_event() function before deleting an event
CREATE TRIGGER before_delete_event_trigger
BEFORE DELETE ON event
FOR EACH ROW
EXECUTE FUNCTION before_delete_event();

-- Create a trigger to call the check_existing_ticket() function before inserting a ticket
CREATE TRIGGER prevent_duplicate_ticket
BEFORE INSERT
ON ticket
FOR EACH ROW
EXECUTE FUNCTION check_existing_ticket();



--============================== Populate Tables ============================================

BEGIN TRANSACTION;

-- Inserting data into the 'event' table
INSERT INTO event VALUES ('E001', 'Concert', 'Arena A', '2024-07-15', '10:00:00', 500);
INSERT INTO event VALUES ('E002', 'Conference', 'Conference Center', '2024-07-20', '09:30:00', 300);
INSERT INTO event VALUES ('E003', 'Sports Event', 'Arena A', '2024-07-15', '15:00:00', 800);
INSERT INTO event VALUES ('E004', 'Festival', 'Park Plaza', '2024-07-25', '12:00:00', 1000);
INSERT INTO event VALUES ('E005', 'Exhibition', 'Arena A', '2024-07-05', '11:00:00', 200);


-- Inserting data into the 'spectator' table
INSERT INTO spectator VALUES (1, 'John Doe', 'john.doe@email.com');
INSERT INTO spectator VALUES (2, 'Jane Smith', 'jane.smith@email.com');
INSERT INTO spectator VALUES (3, 'Bob Johnson', 'bob.john@email.com');
INSERT INTO spectator VALUES (4, 'Alice Williams', 'alice.will@email.com');
INSERT INTO spectator VALUES (5, 'liam Brown', 'liam.brown@email.com');

-- Inserting data into the 'ticket' table
INSERT INTO ticket VALUES (101, 'E001', 1);
INSERT INTO ticket VALUES (102, 'E002', 2);
INSERT INTO ticket VALUES (103, 'E003', 3);
INSERT INTO ticket VALUES (104, 'E004', 4);
INSERT INTO ticket VALUES (105, 'E004', 2);
-- cancelled tickets
INSERT INTO ticket VALUES (106, 'E002', 1);
INSERT INTO ticket VALUES (107, 'E001', 3);
INSERT INTO ticket VALUES (108, 'E002', 4);
INSERT INTO ticket VALUES (109, 'E005', 5);
INSERT INTO ticket VALUES (110, 'E001', 2);

-- Inserting data into the 'cancel' table
INSERT INTO cancel VALUES (106, 'E001', 1, CURRENT_TIMESTAMP, 'admin');
INSERT INTO cancel VALUES (107, 'E001', 3, CURRENT_TIMESTAMP, 'admin');
INSERT INTO cancel VALUES (108, 'E004', 4, CURRENT_TIMESTAMP, 'admin');
INSERT INTO cancel VALUES (109, 'E005', 5, CURRENT_TIMESTAMP, 'admin');
INSERT INTO cancel VALUES (110, 'E002', 2, CURRENT_TIMESTAMP, 'admin');


-- Commit the transaction
COMMIT;

--============================== SELECTS ============================================ 
select * from event;
select * from spectator;
select * from ticket;
select * from cancel;

--============================== DROPS ============================================
drop function check_ticket_status(INTEGER);

drop trigger before_delete_spectator on spectator;
drop function check_spectator_tickets();

drop trigger before_delete_event_trigger on event;
drop function before_delete_event();

drop trigger prevent_duplicate_ticket on ticket;
drop function check_existing_ticket();

drop table cancel;
drop table ticket;
drop table event;
drop table spectator;




--============================== DML - CW ============================================ 
--A
select * from spectator;
INSERT INTO spectator VALUES (6, 'Mike Silver', 'mike.sil@email.com');
select * from spectator;

--B
select * from event;
INSERT INTO event VALUES ('R100', '100M race', 'Sports Park', '2024-07-15', '10:00:00', 700);
select * from event;

--C - Delete a spectator if all their tickets are cancelled
select t.*, sp.sname from spectator sp, ticket t where sp.sno=t.sno and sp.sno = 1 ;
delete from spectator where sno = 1
select t.*, sp.sname from spectator sp, ticket t where sp.sno=t.sno and sp.sno = 6 ;
delete from spectator where sno = 6;
select * from spectator;


--D - Delete an Event if all it's tickets are cancelled
select t.*, e.edesc from event e, ticket t where e.ecode=t.ecode and e.ecode = 'E001' ;
delete from event where ecode = 'E001'
select t.*, e.edesc from event e, ticket t where e.ecode=t.ecode and e.ecode = 'R100' ;
delete from event where ecode = 'R100'
select * from event;

--E
select * from ticket where ecode='E002' and sno=2;
INSERT INTO ticket VALUES (110, 'E002', 2);
select * from ticket where ecode='E002' and sno=3;
INSERT INTO ticket VALUES (111, 'E002', 3);
select * from ticket where ecode='E002' and sno=3;

--F
select e.elocation, e.edate, count(t.sno)
from event e, ticket t, spectator s
where e.ecode=t.ecode
and s.sno=t.sno
group by e.elocation, e.edate

--G
select e.edesc, count(t.tno)
from event e, ticket t
where e.ecode=t.ecode
group by e.edesc

--H
select e.edesc, count(t.tno)
from event e, ticket t
where e.ecode='E001'
and e.ecode=t.ecode
group by e.edesc

--I
select s.sname, e.edate, e.elocation, e.etime, e.edesc
from spectator s, ticket t, event e
where s.sno = 1
and s.sno=t.sno
and t.ecode=e.ecode
order by s.sno, e.edate, e.etime;

--J
select t.tno, s.sname, t.ecode,
    case
        when check_ticket_status(t.tno) THEN 'Valid'
        else 'Cancelled'
    end as ticket_status
from ticket t, spectator s
where t.sno = s.sno
and t.tno = 105;
	
--K
SELECT t.tno, e.ecode, s.sname, c.cdate, c.cuser
FROM ticket t, event e, spectator s, cancel c
WHERE t.ecode = 'E001'
AND t.tno = c.tno
AND t.ecode = c.ecode
AND t.ecode = e.ecode
AND t.sno = s.sno;


--L
-- Start a transaction
BEGIN TRANSACTION;

-- Delete contents of the tables
DELETE FROM cancel;
DELETE FROM ticket;
DELETE FROM spectator;
DELETE FROM event;

-- Commit the transaction
COMMIT;



