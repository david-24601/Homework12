import sqlite3
import pandas as pds
global EnvironmentDir
EnvironmentDir = '\\fileserver\\data\\David\\Documents\\College Classes\\FALL 2024\\Python\\HW12 Git'


def importBooksDB():
    global EnvironmentDir
    # Load Database
    databaseDir = EnvironmentDir+ '\\books.db'
    database = sqlite3.connect('books (1).db')

    # Import Relevant tables
    authorsDF = pds.read_sql('SELECT * FROM authors', database, index_col=['id'])
    titlesDF = pds.read_sql('SELECT * FROM titles', database, index_col=['isbn'])
    isbnDF = pds.read_sql('SELECT * FROM author_ISBN',database, index_col=['id'])

    return authorsDF, titlesDF, isbnDF

def qry(database, table=None, fields=["*"],innerJoin = None, orderBy = None,custom = None):
    '''
    Function creates a dataframe based on the results of a SQL Query.

    Parameters:
    database (str): the directory of the database with relevant tables
    table (str): the name of the primary table to the query
    fields (list) (optional, *): the columns relevant to the query
    innerJoin (str) (optional, None): inner join statement to gather other table data
    orderBY (str) (optional, None): Sort statement to organize data
    custom (str) (optional, None): A SQL statement to preform any other query. Note Overrides all other parameters, and must be compatable with pandas' read_sql function

    returns
    DataFrame filled with query results.
    '''
    if custom != None:
        qry = pds.read_sql(custom,database)
    '''
    # Extra functionality requiring sqlalchemy debugging #
    elif custom == None and (database == None or table == None):
        raise Exception("ERROR: Database directory and Table unknown")
    elif innerJoin == None and orderBy == None:
        qry = pds.read_sql(f'SELECT {*fields,} FROM {table}',database)
    elif orderBy == None and innerJoin != None:
        qry = pds.read_sql(f'SELECT {*fields,} FROM {table} INNER JOIN {innerJoin}',database)
    elif innerJoin == None and fields != None:
        qry = pds.read_sql(f'SELECT {*fields,} FROM {table} ORDER BY {orderBy}',database)
    else:
        qry = pds.read_sql(f'SELECT {*fields,} FROM {table} INNER JOIN {innerJoin} ORDER BY {orderBy}',database)
    '''
    return qry

def initSaveData():
    directory = input('Enter the filename data should be saved to: ')
    return directory

def getAuthorInfo():
    print('Please Enter the following required information to add a book')
    authFirst = input('Author first name: ')
    authLast = input('Author last name: ')
    isbn = int(input('Book ISBN Number: ')) 
    title = input('Book Title: ') 
    tryAgain = True
    while tryAgain:
        try:
            ed = int(input('Edition number: '))
            if ed < 1:
                ed = ed * -1
            tryAgain = False
        except ValueError:
            print('Invalid edition number, please enter a whole number')
            
    tryAgain = True
    while tryAgain:
        try:
            cr = int(input('Copyright Year: '))
            if cr < 1:
                cr = cr * -1
            tryAgain = False
        except ValueError:
            print('Invalid year, please enter a whole number')


    return authFirst, authLast, isbn, title, ed, cr

def initSaveData():
    saveFile = input("Please enter the location output data should be stored in: ")
    return saveFile
def main():
    # constants
    BOOKS_DB_DIR = 'books (1).db'
    
    # Initializing Program
    books_db = sqlite3.connect(BOOKS_DB_DIR)
    authorsDF, titlesDF, isbnDF = importBooksDB()
    # User driven initialization
    saveFile = initSaveData()

    # Program Start
    with open(saveFile,'w') as output:

        # Getting last names of each author in descending order
        authorLastNames = str(qry(database= books_db,custom='SELECT last FROM authors ORDER BY last DESC'))
        print (authorLastNames)
        print('\n')
        output.write(authorLastNames)
        output.write('\n')

        # Getting book titles in ascending order
        bookTitles = str(qry(database= books_db,custom='SELECT title FROM titles ORDER BY title'))
        print (bookTitles)
        print('\n')
        output.write(bookTitles)
        output.write('\n')

        # Getting author, title, Copyright, and ISBN by title
        pubInfo = str(qry(database= books_db, custom = """SELECT first, last, title, copyright, author_ISBN.isbn 
                            FROM author_ISBN
                            INNER JOIN authors
                                ON author_ISBN.id = authors.id
                            INNER JOIN titles
                                ON author_ISBN.isbn = titles.isbn
                            ORDER BY title"""))
        
        print (pubInfo)
        print('\n')
        output.write(pubInfo)
        output.write('\n')

        # Adding Author information
        print('Adding new book')
        output.write('Adding new book')
        authFirst, authLast, isbn, title, ed, cr = 'first','last','12121212','title','1' ,'2024' #getAuthorInfo()
        cursor = books_db.cursor()
        cur = cursor.execute(f"""INSERT INTO authors (first, last)
                                VALUES 
                                    (?,?)""",(authFirst,authLast))
        cur = cursor.execute("""INSERT INTO titles (isbn, title, edition, copyright)
                                VALUES (?,?,?,?)""", (isbn,title,ed,cr))
        Id = len(qry(database= books_db,custom='SELECT last FROM authors'))
        cur = cursor.execute(f"""INSERT INTO author_ISBN (id,isbn)
                                VALUES (?,?)""",(Id,isbn))
        
        pubInfo = str(qry(database= books_db, custom = """SELECT first, last, title, copyright, author_ISBN.isbn 
                            FROM author_ISBN
                            INNER JOIN authors
                                ON author_ISBN.id = authors.id
                            INNER JOIN titles
                                ON author_ISBN.isbn = titles.isbn
                            ORDER BY title"""))
        print(f'Adding {title} by {authFirst} {authLast} ISBN: {isbn} Edition: {ed} Published in {cr}')
        output.write(f'Adding {title} by {authFirst} {authLast} ISBN: {isbn} Edition: {ed} Published in {cr}')
        print (pubInfo)
        print('\n')
        output.write(pubInfo)
        output.write('\n')

        # Using cursor method to parse titles table
        cur = cursor.execute("SELECT * FROM titles")
        fetchallRes = str(cur.fetchall())
        print('Displaying the titles table via fetchall method and description attribute')
        output.write('Displaying the titles table via fetchall method and description attribute')
        print(fetchallRes)
        output.write(fetchallRes)
        print(cur.description)
        output.write(str(cur.description))






main()
print ("HW12 Complete")
