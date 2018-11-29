import psycopg2
import datetime
import configparser
import os
import sys

global dBobjects
dBobjects = []

def getDBobjects(conn_string, viewName):
    schema,view = viewName.split('.')

    level = 0

    def traverseDbDependencies(conn, schemaName, viewName, level):
        inLevel = level
        inLevel = inLevel + 1
        cur = conn.cursor()
        sql_select = "SELECT dependent_schemaname, dependent_objectname from admin.v_object_dependency where src_objectname = '{0}' and src_schemaname = '{1}';".format(viewName,schemaName)
        print(sql_select)
        cur.execute(sql_select)
        for row in cur.fetchall():
            dBobjects.append([row[0],row[1],inLevel])
            traverseDbDependencies(conn, row[0], row[1], inLevel)
        cur.close()


    conn = psycopg2.connect(conn_string)
    traverseDbDependencies(conn,schema,view, level)

    conn.close()
    print(dBobjects)


def printScript(conn_string, viewName):
    maxRank =  max(rank[2] for rank in dBobjects)
    currentRank = maxRank
    schema,view = viewName.split('.')

    changeScript = open("changeScript.sql","w",encoding="utf-8")
    conn = psycopg2.connect(conn_string)

    #generate drop statements
    while currentRank >= 1:
        for row in [item for item in dBobjects if item[2] == currentRank]:
            # drop statement
            changeScript.write("DROP VIEW {0}.{1};\n".format(row[0],row[1]))
        currentRank = currentRank - 1

    #generate changing view statements
    changeScript.write("\n\n------------------------------------------------\n\n-- TODO: CHANGE THIS PART HERE\n\n\n\n")

    cur = conn.cursor()

    # TODO needs to make this available for both tables and views
    ddl_select = "select ddl from admin.v_generate_view_ddl where schemaname = '{0}' and viewname = '{1}';".format(schema,view)
    cur.execute(ddl_select)
    viewDdl = cur.fetchone()[0] + ";\n\n"
    changeScript.write(viewDdl)

    ddl_owner = "SELECT 'ALTER TABLE ' || schemaname || '.' || viewname || ' OWNER TO ' || viewowner || ';' AS ddl FROM pg_views WHERe schemaname = '{0}' AND viewname = '{1}';".format(schema,view)
    cur.execute(ddl_owner)
    viewDdl = cur.fetchone()[0] + "\n\n"
    changeScript.write(viewDdl)

    grant_select = "select ddl from admin.v_generate_user_grant_revoke_ddl where schemaname = '{0}' and objname = '{1}' and ddltype = 'grant';".format(schema,view)
    cur.execute(grant_select)
    for row in cur.fetchall():
        changeScript.write(row[0] + "\n")

    changeScript.write("\n\n")
    cur.close()


    changeScript.write("\n\n------------------------------------------------\n\n")

    #generate create depenencies statements
    while currentRank <= maxRank:
        for row in [item for item in dBobjects if item[2] == currentRank]:
            # create statements
            print("getting script for {0}.{1}".format(row[0],row[1]))

            cur = conn.cursor()

            ddl_select = "select ddl from admin.v_generate_view_ddl where schemaname = '{0}' and viewname = '{1}';".format(row[0],row[1])
            cur.execute(ddl_select)
            viewDdl = cur.fetchone()[0] + "\n\n"
            changeScript.write(viewDdl)

            ddl_owner = "SELECT 'ALTER TABLE ' || schemaname || '.' || viewname || ' OWNER TO ' || viewowner || ';' AS ddl FROM pg_views WHERe schemaname = '{0}' AND viewname = '{1}';".format(row[0],row[1])
            cur.execute(ddl_owner)
            viewDdl = cur.fetchone()[0] + "\n\n"
            changeScript.write(viewDdl)

            grant_select = "select ddl from admin.v_generate_user_grant_revoke_ddl where schemaname = '{0}' and objname = '{1}' and ddltype = 'grant';".format(row[0],row[1])
            cur.execute(grant_select)
            for row in cur.fetchall():
                changeScript.write(row[0] + "\n")

            changeScript.write("\n\n")
            cur.close()

        currentRank = currentRank + 1

    conn.close()


if __name__ == "__main__":

    if len(sys.argv)>2 and str(sys.argv[2]).strip() != "":
        conn_string = sys.argv[2]
        print("Connecting to: {0}".format(conn_string))
    else:
        if os.environ.get('REDSHIFT_CON_STRING'):
            conn_string = os.environ.get('REDSHIFT_CON_STRING')
            print("Connecting to: {0}".format(conn_string))
        else:
            print("No connection string defined")
            exit()

    viewName = 'vmc.sddc'

    if viewName == '':
        if sys.argv[0]:
            viewName = sys.argv[1]
            print("Running script for {0}".format(viewName))

            getDBobjects(conn_string, viewName)
            printScript(conn_string, viewName)

        else:
            print("No object defined")
    else:
        getDBobjects(conn_string, viewName)
        printScript(conn_string, viewName)

