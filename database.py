from sqlalchemy import *
from sqlalchemy.pool import QueuePool
import base64

from Crypto.Cipher import AES
from Crypto.Util import Padding

# pip install sqlalchemy
# pip install pycryptodome
# pip install pymssql

def get_plain_pass(pass_string):
    ret_pass = None
    try:
        iv = b'0000000000000000' # Initialization vector : discussed later
        # AES256
        # 32bytes = AES256, 16bytes = AES128
        user_key = b'\x12\x34\x56\x78\x9a\xbc\xde\xf0\x34\x56\x78\x9a\xbc\xde\xf0\x12' +\
                   b'\x56\x78\x9a\xbc\xde\xf0\x12\x34\x78\x9a\xbc\xde\xf0\x12\x34\x56'

        encrypts = AES.new(user_key, AES.MODE_CBC, IV=iv)

        byte_password = bytes(pass_string, 'utf-8') # str -> byte
        pad_password = Padding.pad(byte_password, AES.block_size)
        encrypted_password = encrypts.encrypt(pad_password)
        base64_password = base64.b64encode(encrypted_password)
        # Check Decoded Password.
        # print('AES B64 encoded = %s' % base64_password)

        # b64decode exception
        decrypter = AES.new(user_key, AES.MODE_CBC, IV=iv)
        encrypted_password = base64.b64decode(pass_string)
        decrypted_password = decrypter.decrypt(encrypted_password)
        ret_pass = Padding.unpad(decrypted_password, AES.block_size).decode('utf-8')  # byte -> str

        # print('AES B64 decoded = %s' % retpass)

    except ValueError as e:
        print(e)
        return None

    return ret_pass

def open_database(database_option):
    # Recovery DB Cursor
    try:
        sql_database_name = database_option['DB_Name']
        sql_host_name = database_option['Host']
        sql_port = database_option['Port']
        sql_user = database_option['User_ID']
        sql_password = database_option['Password']
    except KeyError as e:
        print('KeyError in application_option Key = %s' %e)
        return None

    # Using Encrypt Password
    sql_password = get_plain_pass(sql_password)

    try:
        # create_engine('mssql+pymssql://scott:tiger@hostname:port/dbname')
        create_string = "mssql+pymssql://{sql_user}:{sql_password}@{sql_host_name}:{sql_port}/{sql_database_name}".format(
            sql_user=sql_user,
            sql_password=sql_password,
            sql_host_name=sql_host_name,
            sql_port=sql_port,
            sql_database_name=sql_database_name
        )
        # echo=True 옵션 추가 시 SQL 전체 로그가 출력됨
        # pool_pre_ping : Automatically connection check when every checked out
        # poolclass 는 QueuePool, NullPool 두가지 선택이 가능하다.
        # pool_size 의 Default 는 5, max_overflow 의 Default 는 10 이다.
        # sql server 의 Maximum number of concurrent connections 는 32767 건이다 (Default == 0 == unlimited == 32,767)
        engine = create_engine(create_string, encoding='utf-8', pool_pre_ping=True, poolclass=QueuePool)
        # db_conn = engine.connect()

    except Exception as e:
        print(e)
        print('Database Connection Failed.')
        print('Check decrypted = %s' % sql_password)
        return None

    return engine

######################## Using Example #############################

database_option = {
    "DB_Name": "AdventureWorks2012",
    "Host": "127.0.0.1",
    "Port": "1433",
    "User_ID": "sa",
    "Password": "password"
}

connection = open_database(database_option)

query_string = '''
    Select top 1 * from [AdventureWorks2012].[Person].[Address]
'''

# Begin
# Query
# Commit / Rollback

trans = connection.begin()
try:
    result_proxy = connection.execute(query_string)
    result_set = result_proxy.fetchall()
    # trans.commit()
    print(result_set)
except:
    trans.rollback()
