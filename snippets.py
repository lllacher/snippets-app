import logging
import argparse
import psycopg2

# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)
logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect(database="snippets")
logging.debug("Database connection established.")

def put(name, snippet):
    """Store a snippet with an associated name."""
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    #cursor = connection.cursor()
    with connection, connection.cursor() as cursor:
        try:
            command = "insert into snippets values (%s, %s)"
            cursor.execute(command, (name, snippet))
        except psycopg2.IntegrityError as e:
            connection.rollback()
            command = "update snippets set message=%s where keyword=%s"
            cursor.execute(command, (snippet, name))
    connection.commit()
    logging.debug("Snippet stored successfully.")
    return name, snippet

def get(name):
    """Retrieve the snippet with a given name.

    If there is no such snippet, return '404: Snippet Not Found'.

    Returns the snippet.
    """
    logging.info("Getting snippet {!r}".format(name))
    # replace following 4 lines with improved code below it
    #cursor = connection.cursor()
    #command = "select keyword, message from snippets where keyword='"+name+"'"
    #cursor.execute(command,)
    #row = cursor.fetchone()
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        row = cursor.fetchone()
    connection.commit()

    logging.debug("Snippet retrieved successfully.")
    if not row:
        return "404: Snippet Not Found"
    return row[0]
    #logging.error("FIXME: Unimplemented - get({!r})".format(name))
    #return name

def catalog():
    """Retrieve and display all snippets."""
    with connection, connection.cursor() as cursor:
        cursor.execute("SELECT keyword, message FROM snippets order by keyword")
        result = cursor.fetchmany()
        while result:
            for keyword, message in result:
                print ("Keyword:",keyword, "   Message:",message)
            result = cursor.fetchmany()
 
def search(word):  
    """Search for snippets which contain a given string anywhere in their message."""
    with connection, connection.cursor() as cursor:
        command="SELECT keyword, message FROM snippets where message like '%"+word+"%' order by keyword"
        cursor.execute(command)
        result = cursor.fetchmany()
        while result:
            for keyword, message in result:
                print ("Keyword:",keyword, "   Message:",message)
            result = cursor.fetchmany()

def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    put_parser.add_argument("snippet", help="Snippet text")
    get_parser = subparsers.add_parser("get", help="Get a snippet")
    get_parser.add_argument("name", help="Name of the snippet")
    catalog_parser = subparsers.add_parser("catalog", help="Display all snippets")
    search_parser = subparsers.add_parser("search", help="Search for a word in the message of all snippets")
    search_parser.add_argument("word", help="Word to search for in snippet message")
    arguments = parser.parse_args()
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))
    elif command == "catalog":
        catalog()
    elif command == "search":
        search(**arguments)
        

if __name__ == "__main__":
    main()

        
