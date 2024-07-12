import mysql.connector as con
from mysql.connector import errorcode
from mysql.connector import Error

# Method for creating a data base project2_1
def create_database(cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS project2")
    cursor.execute("USE project2")

# Method for creating a table publishers
def create_table_publishers(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS publishers (
            pubID INT(3) NOT NULL,
            pname VARCHAR(30) NULL,
            email VARCHAR(50) NULL,
            phone VARCHAR(30) NULL,
            PRIMARY KEY (pubID),
            UNIQUE INDEX email_unique (email ASC) VISIBLE
                    
        )
    """)
    print("Table publishers created or already exists")

# Method for inserting the records in to publisher table
def insert_records_publishers(cursor):
    records = [
        (1, 'WILLEY', 'WDT@VSNL.NET', '9112326087'),
        (2, 'WROX', 'INFO@WROX.COM', None),
        (3, 'TATA MCGRAW-HILL', 'FEEDBACK@TATAMCGRAWHILL.COM', '9133333322'),
        (4, 'TECHMEDIA', 'BOOKS@TECHMEDIA.COM', '9133257660')
    ]
    insert1="INSERT IGNORE INTO publishers (pubID, pname, email, phone) VALUES (%s, %s, %s, %s)"
    cursor.executemany(insert1, records)
    print(f"{cursor.rowcount} records inserted into publishers table")



# Method for creating a table subjects
def create_table_subjects(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS subjects (
            subID VARCHAR(5) NOT NULL,
            sName VARCHAR(50) NULL,
            PRIMARY KEY (subID)
        )
    """)
    print("Table subjects created or already exists")

def insert_records_subjects(cursor):
    records = [
        ('ORA', 'ORACLE DATABASE'),
        ('JAVA', 'JAVA LANGUAGE'),
        ('JEE', 'JAVA ENTERPRISE EDITION'),
        ('VB', 'VISUAL BASIC.NET'),
        ('ASP', 'ASP.NET')
    ]
    insert_query = "INSERT IGNORE INTO subjects (subID, sName) VALUES (%s, %s)"
    cursor.executemany(insert_query, records)
    print(f"{cursor.rowcount} records inserted into subjects table")

"""     table 3 """
# Method for creating a table authors
def create_table_authors(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS authors (
            auID INT(5) NOT NULL,
            aName VARCHAR(30) NULL,
            email VARCHAR(50) NULL,
            phone VARCHAR(30) NULL,
            PRIMARY KEY (auID),
            UNIQUE INDEX email_UNIQUE(email ASC) VISIBLE
        )
    """)
    print("Table authors created or already exists")

def insert_records_authors(cursor):
    records = [
        (101, 'HERBERT SCHILD', 'HERBERT@YAHOO.COM', '2137823450'),
        (102, 'JAMES GOODWILL', 'GOODWILL@HOTMAIL.COM', '9095871243'),
        (103, 'DAVAID HUNTER', 'HUNTER@HOTMAIL.COM', '9094235581'),
        (104, 'STEPHEN WALTHER', 'WALTHER@GMAIL.COM', '2138773902'),
        (105, 'KEVIN LONEY', 'LONEY@ORACLE.COM', '9493423410'),
        (106, 'ED. ROMANS', 'ROMANS@THESERVERSIDE.COM', '9495012201')
    ]
    insert_query = "INSERT IGNORE INTO authors (auID, aName, email, phone) VALUES (%s, %s, %s, %s)"
    cursor.executemany(insert_query, records)
    print(f"{cursor.rowcount} records inserted into authors table")

""" TABLE 4    """
# Method for creating a table titles
def create_table_titles(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS titles (
            titleID INT(5) NOT NULL,
            title VARCHAR(30) NULL,
            pubID INT(3) NULL,
            subID VARCHAR(5) NULL,
            pubDate DATE NULL,
            cover VARCHAR(10) NULL,
            price INT(4) NULL,
            PRIMARY KEY (titleID),
            INDEX pubid_idx (pubID ASC) VISIBLE,
            INDEX subid_idx (subID ASC) VISIBLE,
            CONSTRAINT pubid
                FOREIGN KEY (pubID)
                REFERENCES publishers(pubID)
                ON DELETE NO ACTION
                ON UPDATE NO ACTION,
            CONSTRAINT subid
                FOREIGN KEY (subID)
                REFERENCES subjects(subID)
                ON DELETE NO ACTION
                ON UPDATE NO ACTION
        )
    """)
    print("Table titles created or already exists")


def insert_records_titles(cursor):
    records = [
        (1001, 'ASP.NET UNLEASHED', 4, 'ASP', '2002-04-02', 'HARD COVER', 540),
        (1002, 'ORACLE10G COMP. REF.', 3, 'ORA', '2005-05-01', 'PAPER BACK', 575),
        (1003, 'MASTERING EJB', 1, 'JEE', '2005-02-03', 'PAPER BACK', 475),
        (1004, 'JAVA COMP. REF', 3, 'JAVA', '2005-04-03', 'PAPER BACK', 499),
        (1005, 'PRO. VB.NET', 2, 'VB', '2005-06-15', 'HARD COVER', 450),
        (1006, 'INTRO. VB.NET', 2, 'VB', '2002-12-02', 'PAPER BACK', 425)
    ]
    insert_query = "INSERT IGNORE INTO titles (titleID, title, pubID, subID, pubDate, cover, price) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cursor.executemany(insert_query, records)
    print(f"{cursor.rowcount} records inserted into titles table")

""" table 5"""
def create_table_titleauthors(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS titleauthors (
            titleID INT(5) NOT NULL,
            auID INT(5) NOT NULL,
            importance INT(2) NULL,
            PRIMARY KEY (titleID, auID),
            INDEX auID_idx (auID ASC) VISIBLE,
            CONSTRAINT titleid
                FOREIGN KEY (titleID)
                REFERENCES titles(titleID)
                ON DELETE NO ACTION
                ON UPDATE NO ACTION,
            CONSTRAINT auid
                FOREIGN KEY (auID)
                REFERENCES authors(auID)
                ON DELETE NO ACTION
                ON UPDATE NO ACTION
        )
    """)
    print("Table titleauthors created or already exists")

def insert_records_titleauthors(cursor):
    records = [
        (1001, 104, 1),
        (1002, 105, 1),
        (1003, 106, 1),
        (1004, 103, 1),
        (1005, 103, 1),
        (1005, 102, 2)
    ]
    insert_query = "INSERT IGNORE INTO titleauthors (titleID, auID, importance) VALUES (%s, %s, %s)"
    cursor.executemany(insert_query, records)
    print(f"{cursor.rowcount} records inserted into titleauthors table")

def create_and_insertMethod(cursor):
    create_table_publishers(cursor)
    insert_records_publishers(cursor)
    create_table_subjects(cursor)
    insert_records_subjects(cursor)
    create_table_authors(cursor)
    insert_records_authors(cursor)
    create_table_titles(cursor)
    insert_records_titles(cursor)
    create_table_titleauthors(cursor)
    insert_records_titleauthors(cursor)
    

def query_1_fetch_recordsFromTitle(cursor):
    cursor.execute("select * from titles")
    select=cursor.fetchall()
    for row in select:
      print(row)

def query_2_create_Customer(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer (
        custID INT PRIMARY KEY AUTO_INCREMENT,
        custName VARCHAR(100),
        zip VARCHAR(10),
        city VARCHAR(100),
        state VARCHAR(100)
    )""")
    print("Table customer created or already exists")

def query_3_insert_records_customer(cursor):
    records = [
        ('ABRAHAM SILBERSCHATZ', '12345', 'CityA', 'StateA'),
        ('HENRY KORTH', '67890', 'CityB', 'StateB'),
        ('CALVIN HARRIS', '54321', 'CityC', 'StateC'),
        ('MARTIN GARRIX', '98765', 'CityD', 'StateD'),
        ('JAMES GOODWILL', '11223', 'CityE', 'StateE')
    ]
    insert_query = "INSERT IGNORE INTO customer (custName, zip, city, state) VALUES ( %s, %s, %s, %s)"
    cursor.executemany(insert_query, records)
    print(f"{cursor.rowcount} records inserted into customer table")



    

def query_4_publisher_titles(cursor):
    cursor.execute("""
        SELECT t.pubID, COUNT(t.pubID) AS no_of_title
        FROM titles t 
        JOIN publishers p ON t.pubID = p.pubID
        GROUP BY t.pubID
        HAVING COUNT(t.pubID) = (
            SELECT MAX(title_count)
            FROM (
                SELECT COUNT(t1.pubID) AS title_count
                FROM titles t1
                JOIN publishers p1 ON t1.pubID = p1.pubID
                GROUP BY t1.pubID
            ) AS max_counts
        )
        ORDER BY no_of_title DESC
    """)
    result = cursor.fetchall()
    for row in result:
      print(f"Publisher ID: {row[0]}, Number of Titles: {row[1]}")

def query_5_listAuthorsByTitle(cursor):
    cursor.execute("""
        select a.auID,a.aName,sum(t.price) as total_price  from authors a join titleauthors ta
        on a.auID=ta.auID join titles t on ta.titleID=t.titleID group by (a.auID) order by total_price desc;
    """)
    result = cursor.fetchall()
    for row in result:
      print(row)


def query_6_list_all_titles(cursor):
    cursor.execute("""
        select t.title from titles t where t.titleID =
        (select ta.titleID from titleauthors ta join authors a on ta.auID=a.auID group by ta.titleID having count(ta.auID)>1);
    """)
    result = cursor.fetchall()
    for row in result:
      print(row)

def query_7_list_all_publishers(cursor):
    cursor.execute("""
        select p.pname from titles t join publishers p on t.pubID=p.pubID where t.price<500 and t.cover='PAPER BACK';
    """)
    result = cursor.fetchall()
    for row in result:
      print(row)

def query_8_list_all_authors(cursor):
    cursor.execute("""
        select a.auId,a.aName from authors a join titleauthors ta on ta.auID=a.auID join titles t on t.titleID=ta.titleID join
        subjects s on s.subID=t.subID  group by  a.auID
        having SUM(s.sName LIKE '%JAVA%') > 0
        AND SUM(s.sName LIKE '%VISUAL BASIC.NET%') = 0;
    """)
    result = cursor.fetchall()
    for row in result:
      print(row)

def query_9_list_all_publishers(cursor):
    cursor.execute("""
        select pname from publishers where email like '%.com'
    """)
    result = cursor.fetchall()
    for row in result:
      print(row)
def query_10_update_titles(cursor,connection):
    cursor.execute("""
        UPDATE titles
        SET price = CASE 
            WHEN pubDate < '2003-01-01' THEN price * 0.95
            WHEN pubDate > '2005-01-01' THEN price * 1.15
            ELSE price
        END;
    """)
    print("updated titles table")
    connection.commit()
    


def all_query_method(cursor,connection):
    print("All Query Methods ")
    #query_1_fetch_recordsFromTitle(cursor)
    query_2_create_Customer(cursor)
    query_3_insert_records_customer(cursor)
    """query_4_publisher_titles(cursor)
    query_5_listAuthorsByTitle(cursor)
    query_6_list_all_titles(cursor)
    query_7_list_all_publishers(cursor)
    query_8_list_all_authors(cursor)
    query_9_list_all_publishers(cursor)
    query_10_update_titles(cursor,connection)"""
   
    

    


try:
    connection = con.connect(host="localhost",
                                  user="root",
                                  password="Chinnu@2024"
                                 )

    if connection.is_connected():
        db_Info=connection.get_server_info()
        print("Connected to MySql Server version ",db_Info)                            
        cursor = connection.cursor()
        #creating database if not exists
        create_database(cursor)
        print("data base created or already exists")
        record=cursor.fetchone()
        print("You 're connected to database: ",record)
        create_and_insertMethod(cursor)
        all_query_method(cursor,connection)
        connection.commit()
except Error as e:
    print("Error while connecting to MySql",e)
finally:
    if(connection.is_connected()):
        cursor.close()
        connection.close()
        print("Mysql connection is closed")

        
