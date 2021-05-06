from app import mysql, session
from blockchain import Block, Blockchain


# Custom exceptions for transaction errors
class InvalidTransactionException(Exception):
    pass


class InsufficientFundsException(Exception):
    pass


# What a mysql table looks like. Simplifies access to the database 'crypto'
class Table():
    # Specify the table name and columns
    #EXAMPLE table:
    #               blockchain
    # number    hash    previous   data    nonce
    # -data-   -data-    -data-   -data-  -data-
    #
    # EXAMPLE initialization: ...Table("blockchain", "number", "hash", "previous", "data", "nonce")
    def __init__(self, table_name, *args):
        self.table = table_name
        self.columns = ",".join(args)
        self.columnsList = args

        # if table does not already exist, create it.
        if isnewtable(table_name):
            create_data = ""
            for column in self.columnsList:
                create_data += f"{column} varchar(100),"

            cur = mysql.connection.cursor()  # Create the table
            cur.execute(
              f"CREATE TABLE {self.table}({create_data[:len(create_data)-1]})")
            cur.close()

    # Get all the values from the table
    def getall(self):
        cur = mysql.connection.cursor()
        result = cur.execute(f"SELECT * FROM {self.table}")
        data = cur.fetchall()
        return data

    # Get one value from the table based on a column's data
    # EXAMPLE using blockchain: ...getone("hash","00003f73gh93...")
    def getone(self, search, value):
        data = {}
        cur = mysql.connection.cursor()
        result = cur.execute(
            f"SELECT * FROM {self.table} WHERE {search} = \"{value}\"")
        if result > 0:
            data = cur.fetchone()
        cur.close()
        return data

    # Delete a value from the table based on column's data
    def deleteone(self, search, value):
        cur = mysql.connection.cursor()
        cur.execute(
            f"DELETE from {self.table} where {search} = \"{value}\"")
        mysql.connection.commit()
        cur.close()

    # Delete all values from the table.
    def deleteall(self):
        self.drop()  # Remove table and recreate
        self.__init__(self.table, *self.columnsList)

    # remove table from mysql
    def drop(self):
        cur = mysql.connection.cursor()
        cur.execute(f"DROP TABLE {self.table}")
        cur.close()

    # insert values into the table
    def insert(self, *args):
        data = ""
        for arg in args:  # Convert data into string mysql format
            data += f"\"{arg}\","

        cur = mysql.connection.cursor()
        cur.execute(
            f"INSERT INTO {self.table}{self.columns} VALUES({data[:len(data)-1]})")
        mysql.connection.commit()
        cur.close()


# Execute mysql code from python
def sql_raw(execution):
    cur = mysql.connection.cursor()
    cur.execute(execution)
    mysql.connection.commit()
    cur.close()


# Check if table already exists
def isnewtable(tableName):
    cur = mysql.connection.cursor()

    try:  # Attempt to get data from table
        result = cur.execute(f"SELECT * from {tableName}")
        cur.close()
    except Exception:
        return True
    else:
        return False


# Check if user already exists
def isnewuser(username):
    # Access the users table and get all values from column "username"
    users = Table("users", "name", "email", "username", "password")
    data = users.getall()
    usernames = []
    for user in data:
        usernames += user.get('username')
    if username in usernames:
        return False
    else:
        return True


# Send money from one user to another
def send_money(sender, recipient, amount):
    # Verify that the amount is an integer or floating value
    try:
        amount = float(amount)
    except ValueError:
        raise InvalidTransactionException("Invalid Transaction.")

    # Verify that the user has enough money to send
    # (exception if it is the BANK)
    if amount > get_balance(sender) and sender != "BANK":
        raise InsufficientFundsException("Insufficient Funds.")

    # Verify that the user is not sending money to
    # themselves or amount is less than or 0
    elif sender == recipient or amount <= 0.00:
        raise InvalidTransactionException("Invalid Transaction.")

    # Verify that the recipient exists
    elif isnewuser(recipient):
        raise InvalidTransactionException("User Does Not Exist.")

    # Update the blockchain and sync to mysql
    blockchain = get_blockchain()
    number = len(blockchain.chain) + 1
    data = f"{sender}-->{recipient}-->{amount}"
    blockchain.mine(Block(number, data=data))
    sync_blockchain(blockchain)


# get the balance of a user
def get_balance(username):
    balance = 0.00
    blockchain = get_blockchain()

    # loop through the blockchain and update balance
    for block in blockchain.chain:
        data = block.data.split("-->")
        if username == data[0]:
            balance -= float(data[2])
        elif username == data[1]:
            balance += float(data[2])
    return balance


# Get the blockchain from mysql and convert to Blockchain object
def get_blockchain():
    blockchain = Blockchain()
    blockchain_sql = Table("blockchain", "number", "hash", "previous", "data", "nonce")
    for b in blockchain_sql.getall():
        blockchain.add(
            Block(
                int(b.get('number')),
                b.get('previous'),
                b.get('data'),
                int(b.get('nonce')),
                ))

    return blockchain


# Update blockchain in mysql table
def sync_blockchain(blockchain):
    blockchain_sql = Table("blockchain", "number", "hash", "previous", "data", "nonce")
    blockchain_sql.deleteall()

    for block in blockchain.chain:
        blockchain_sql.insert(
            str(block.number),
            block.hash(),
            block.previous_hash,
            block.data,
            block.nonce
        )
