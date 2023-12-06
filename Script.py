'''
Before you can run this code, please do the following:
    Fill up the information in line 23 below "connStr = "host='cmpstudb-01.cmp.uea.ac.uk' dbname= '' user='' password = " + pw"
    using the Russell Smith's provided credentials', add your password in the pw.txt file.

    Get connected to VPN (https://my.uea.ac.uk/divisions/it-and-computing-services/service-catalogue/network-and-telephony-services/vpn) if you are running this code from an off-campus location.

    Get the server running in https://pgadmin.cmp.uea.ac.uk/ by log into the server.

'''

import psycopg2
import pandas as pd


def getConn():
    pwFile = open("pw.txt", "r")
    pw = pwFile.read()
    pwFile.close()
    connStr = "host='cmpstudb-01.cmp.uea.ac.uk' \
               dbname='ygb19mzu' user='ygb19mzu' password=" + pw
    conn = psycopg2.connect(connStr)
    return conn


def clearOutput():
    with open("output2.txt", "w") as clearfile:
        clearfile.write('')


def writeOutput(output):
    with open("output2.txt", "a") as myfile:
        myfile.write(output)

def split_text(task_text):
    raw = task_text.split("#", 1)
    raw[1] = raw[1].strip()
    data = raw[1].split("#")
    return data

def execute_sql(conn, cur, sql):
    try:
        # Set search path to ygb19mzu_library_cw
        cur.execute("SET SEARCH_PATH TO ygb19mzu_library_cw, public;")
        # Execute SQL query
        cur.execute(sql)
        # Write Success/Failure message
        writeOutput(cur.statusmessage + "\n")
    except Exception as e:
        writeOutput(str(e) + "\n")

def write_output(conn, sql):
    try:
        # Execute select query
        table_df = pd.read_sql_query(sql, conn)
        # Turn the query into a string
        table_str = table_df.to_string()
        # Write the string to the txt file
        writeOutput(table_str + "\n")
    except Exception as e:
        writeOutput(str(e) + "\n")


conn = None
try:
    conn = getConn()
    conn.autocommit = True
    cur = conn.cursor()

    # Set search path to ygb19mzu_library_cw
    cur.execute("SET SEARCH_PATH TO ygb19mzu_library_cw, public;")

    f = open("input2.txt", "r")
    clearOutput()

    for x in f:
        print(x)
        if x[0] == 'A':
            data = split_text(x)
            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all spectaor before adding new spectator
                sql = "SELECT * FROM spectator"
                write_output(conn, sql)


                # SQL insert new Spectator query
                sql = "INSERT INTO spectator VALUES ({}, '{}', '{}');".format(data[0], data[1], data[2])
                # insert spectaor
                execute_sql(conn, cur, sql)

                # SQL query to show spectaor was successfully added
                sql = "SELECT * FROM spectator"
                write_output(conn,sql)

                # Space
                print("\n\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

        elif x[0] == 'B':
            data = split_text(x)
            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all events before adding new event
                sql = "SELECT * FROM event"
                # Execute select query
                write_output(conn, sql)

                # Insert the new event
                sql = "INSERT INTO event VALUES ('{}', '{}', '{}', '{}', '{}', {});".format(data[0], data[1], data[2], data[3], data[4], data[5])
                # Insert Event
                execute_sql(conn, cur, sql)

                # SQL query to show spectaor was successfully added
                sql = "SELECT * FROM event"
                # Execute select query
                write_output(conn, sql)

                # Space
                print("\n\n")
            except Exception as e:
                writeOutput(str(e) + "\n")


        elif x[0] == 'C':
            raw = x.split("#", 1)
            raw[1] = raw[1].strip()
            data = raw[1].split("#")

            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all events before adding new event
                sql = "select t.*, sp.sname from spectator sp, ticket t where sp.sno=t.sno and sp.sno = 1 ;"
                write_output(conn, sql)

                # Delete spectator query
                sql = "delete from spectator where sno = 1"
                execute_sql(conn, cur, sql)

                # SQL query to show all events before adding new event
                sql = "select t.*, sp.sname from spectator sp, ticket t where sp.sno=t.sno and sp.sno = 6 ;"
                write_output(conn, sql)

                # Delete spectator query
                sql = "delete from spectator where sno ={}".format(data[0])
                execute_sql(conn, cur, sql)

                # SQL query to show spectaor was successfully added
                sql = "select * from spectator;"
                write_output(conn, sql)

                # Space
                print("\n\n")
            except Exception as e:
                writeOutput(str(e) + "\n")


        elif x[0] == 'D':
            data = split_text(x)

            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all events before adding new event
                sql = "select t.*, e.edesc from event e, ticket t where e.ecode=t.ecode and e.ecode = 'E001' ;"
                write_output(conn, sql)

                # Insert the new event
                sql = "delete from event where ecode = 'E001'"
                execute_sql(conn, cur, sql)

                # SQL query to show all events before adding new event
                sql = "select t.*, e.edesc from event e, ticket t where e.ecode=t.ecode and e.ecode = 'R100' ;"
                write_output(conn, sql)

                # Insert the new event
                sql = "delete from event where ecode = '{}'".format(data[0])
                execute_sql(conn, cur, sql)

                # SQL query to show spectaor was successfully added
                sql = "select * from event;"
                write_output(conn, sql)

                # Space
                print("\n\n")

            except Exception as e:
                writeOutput(str(e) + "\n")

        elif x[0] == 'E':
            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all events before adding new event
                sql = "select * from ticket where ecode='E002' and sno=2;"
                write_output(conn, sql)

                # Insert the new event
                sql = "INSERT INTO ticket VALUES (110, 'E002', 2);"
                execute_sql(conn, cur, sql)

                # SQL query to show all events before adding new event
                sql = "select * from ticket where ecode='E002' and sno=3;"
                write_output(conn, sql)

                # Insert the new event
                sql = "INSERT INTO ticket VALUES (111, 'E002', 3);"
                execute_sql(conn, cur, sql)

                # SQL query to show spectaor was successfully added
                sql = "select * from ticket where ecode='E002' and sno=3;"
                write_output(conn, sql)

                # Space
                print("\n\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

        elif x[0] == 'F':
            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all events before adding new event
                sql = """   
                        select e.elocation, e.edate, count(t.sno)
                        from event e, ticket t, spectator s
                        where e.ecode=t.ecode
                        and s.sno=t.sno
                        group by e.elocation, e.edate
                    """
                execute_sql(conn, cur, sql)

                write_output(conn, sql)

                # Space
                print("\n\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

        elif x[0] == 'G':
            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all events before adding new event
                sql = """   
                        select e.edesc, count(t.tno)
                        from event e, ticket t
                        where e.ecode=t.ecode
                        group by e.edesc
                    """
                execute_sql(conn, cur, sql)

                write_output(conn, sql)

                # Space
                print("\n\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

        elif x[0] == 'H':
            data = split_text(x)
            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all events before adding new event
                sql = """   
                        select e.edesc, count(t.tno)
                        from event e, ticket t
                        where e.ecode='{}'
                        and e.ecode=t.ecode
                        group by e.edesc
                    """.format(data[0])
                execute_sql(conn, cur, sql)

                write_output(conn, sql)

                # Space
                print("\n\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

        elif x[0] == 'I':
            data = split_text(x)
            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all events before adding new event
                sql = """   
                        select s.sname, e.edate, e.elocation, e.etime, e.edesc
                        from spectator s, ticket t, event e
                        where s.sno = {}
                        and s.sno=t.sno
                        and t.ecode=e.ecode
                        order by s.sno, e.edate, e.etime;
                    """.format(data[0])
                execute_sql(conn, cur, sql)

                write_output(conn, sql)

                # Space
                print("\n\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

        elif x[0] == 'J':
            data = split_text(x)
            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all events before adding new event
                sql = """
                    select t.tno, s.sname, t.ecode,
                        case
                            when check_ticket_status(t.tno) THEN 'Valid'
                            else 'Cancelled'
                        end as ticket_status
                    from ticket t, spectator s
                    where t.sno = s.sno
                    and t.tno = {};
                """.format(data[0])
                execute_sql(conn, cur, sql)

                write_output(conn, sql)

                # Space
                print("\n\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

        elif x[0] == 'K':
            data = split_text(x)
            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all events before adding new event
                sql = """
                    SELECT t.tno, e.ecode, s.sname, c.cdate, c.cuser
                    FROM ticket t, event e, spectator s, cancel c
                    WHERE t.ecode = 'E001'
                    AND t.tno = c.tno
                    AND t.ecode = c.ecode
                    AND t.ecode = e.ecode
                    AND t.sno = s.sno;
                """.format(data[0])
                execute_sql(conn, cur, sql)

                write_output(conn, sql)

                # Space
                print("\n\n")
            except Exception as e:
                writeOutput(str(e) + "\n")

        elif x[0] == 'L':
            try:
                # Write the task name
                writeOutput("TASK " + x[0] + "\n")

                # SQL query to show all events before adding new event
                sql = """
                    -- Start a transaction
                    BEGIN TRANSACTION;
                    
                    -- Delete contents of the tables
                    DELETE FROM cancel;
                    DELETE FROM ticket;
                    DELETE FROM spectator;
                    DELETE FROM event;
                    
                    -- Commit the transaction
                    COMMIT;
                """.format(data[0])
                execute_sql(conn, cur, sql)

                write_output(conn, sql)
                # Space
                print("\n\n")
            except Exception as e:
                writeOutput(str(e) + "\n")



    print("Exit {}".format(x[0]))
    writeOutput("\n\nExit program!")

except Exception as e:
    print(e)
