"""
# Drop keyspace amazon_data

CREATE KEYSPACE amazon_data
WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};

use amazon_data;

create  table subdepts_t1(
                  depart_name text,
                  subdepart_name text,
                  product_id text,
                  product_name text,
                  product_price text,
                  week_number text,
                  primary key(depart_name, subdepart_name, product_id));

insert into subdepts_t1(depart_name,subdepart_name,product_id, product_name,product_price , week_number) values('Amazon Launchpad','Body','B004QQ9LVS', 'SmartyPants Kids Complete Gummy Vitam...','$17.95', 'week1');

Select * from subdepts_t1;

"""
#%%

## installation
## pip install cassandra-driver -- This will install the latest cassandra driver,
## some times we may get compatibility errors with respect to the python version installed in our local machine
## then try to install downgraded version 
## pip install https://pypi.python.org/packages/source/c/cassandra-driver/cassandra-driver-2.7.2.tar.gz

## ## pip install cqlengine
## Github document: https://github.com/Aravindreddy986/Cassandra
## Source for this Video : https://academy.datastax.com/resources/getting-started-apache-cassandra-and-python-part-i

## Features provided with this driver:

## Synchronous and Asynchronous APIs
## Simple, Prepared, and Batch statements
## Asynchronous IO, parallel execution, request pipelining
## Connection pooling
## Automatic node discovery
## Automatic reconnection
## Configurable load balancing and retry policies
## Concurrent execution utilities
## Object mapper

from cqlengine import columns
from cqlengine import connection
from cassandra import ReadTimeout
from cqlengine.models import Model
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cqlengine.management import sync_table
from cassandra.policies import DCAwareRoundRobinPolicy,TokenAwarePolicy,RetryPolicy


#%%

## The below commands will create an instance of the cassandra cluster to which i want to interact
## Similar to how we connect to SQL server instance using SSMS

#cluster = Cluster()

#cluster = Cluster( contact_points=['127.0.0.1'] )
#session = cluster.connect('amazon_data')

cluster = Cluster(
  contact_points=['127.0.0.1'],
   load_balancing_policy= TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')),
   default_retry_policy = RetryPolicy()  )

## Now the below command will help us creating a session
## nothing but helping us in executing queries against a table
## in a keyspace to which we have connected by creating a session
session = cluster.connect('amazon_data')

#session.set_keyspace('users')
# or you can do this instead
#session.execute('amazon_data1')

#%%

data = session.execute(""" select * from subdepts_t1 """)

print str(type(data)) + " ----  " + str(len(data))
for each in data:
    print each
    
print "#####################################"

data_1 = session.execute(""" select * from subdepts_t1 """)[0]
print str(type(data_1)) + " ----  " + str(len(data_1))

for each in data_1:
    print each

#%%

print "Inserting a row in the database"

session.execute(""" insert into subdepts_t1(depart_name,subdepart_name,product_id, product_name, product_price ,week_number)
            values( 'Amazon Launchpad', 'Body',	'B00CX6DM0O','SmartyPants Kids Complete Fiber Multi...', '$28','week1'); """)
    
session.execute(""" insert into subdepts_t1(depart_name,subdepart_name,product_id, product_name,product_price , week_number)
                    values('Amazon Launchpad','Body','B01B1JPXBE', 'SmartyPants Womens Complete Gummy Vi...','$34.95', 'week1');""")
            
data = session.execute(""" select * from subdepts_t1 """)[1]

print str(type(data)) + " ----  " + str(len(data))
for each in data:
    print each

print("Updating the records")
session.execute("update subdepts_t1 set product_price = '$42' where depart_name = 'Amazon Launchpad' AND subdepart_name = 'Body' AND product_id= 'B00CX6DM0O' ")
result = session.execute(" select * from subdepts_t1  where depart_name = 'Amazon Launchpad' AND subdepart_name = 'Body' AND product_id= 'B00CX6DM0O' ")[0]

for each in result:
    print each

print("#######################################")
print("Deleting the records")
session.execute("delete from subdepts_t1  where depart_name = 'Amazon Launchpad' AND subdepart_name = 'Body' AND product_id= 'B00CX6DM0O' ")
data = session.execute(""" select * from subdepts_t1 """)

print str(type(data)) + " ----  " + str(len(data))
for each in data:
    print each

#%% Prepared Statements 

## Preparing the statement
## Binding the variables
## Executing the statement

print("#################### Insertion ###################### \n")
prepared_stmt = session.prepare ( "insert into subdepts_t1 (depart_name,subdepart_name,product_id, product_name, product_price , week_number ) VALUES( ?, ?, ?, ?, ?, ?) ")

bound_stmt = prepared_stmt.bind([ 'Amazon Launchpad',	'Body', 'B00VFYYAC4',	'SmartyPants PreNatal Complete Gummy V...', '$21.95','week1'])
stmt = session.execute(bound_stmt)

bound_stmt = prepared_stmt.bind([ 'Amazon Launchpad','Body','B005LR7WOY','SmartyPants Baby Complete Gummy Vi...',	'$21.23','week1'])
stmt = session.execute(bound_stmt)

data = session.execute(""" select * from subdepts_t1 """)
for each in data:
    print each

print("##################### Updation ##################### \n")
prepared_stmt = session.prepare ("UPDATE subdepts_t1 set product_price = ? where depart_name = ? AND subdepart_name = ? AND product_id= ? ")
bound_stmt = prepared_stmt.bind(['$59','Amazon Launchpad','Body','B005LR7WOY'])
stmt = session.execute(bound_stmt)

prepared_stmt = session.prepare(""" select * from subdepts_t1 where depart_name = ? AND subdepart_name = ? AND product_id= ? """)
bound_stmt = prepared_stmt.bind(['Amazon Launchpad','Body','B005LR7WOY'])
data = session.execute(bound_stmt)

for each in data:
    print each


print("##################### Deletion ##################### \n")
prepared_stmt = session.prepare ("Delete from subdepts_t1 where depart_name = ? AND subdepart_name = ? AND product_id= ? ")
bound_stmt = prepared_stmt.bind(['Amazon Launchpad','Body','B005LR7WOY'])
stmt = session.execute(bound_stmt)

prepared_stmt = session.prepare(""" select * from subdepts_t1 where depart_name = ? AND subdepart_name = ? AND product_id= ? """)
bound_stmt = prepared_stmt.bind(['Amazon Launchpad','Body','B005LR7WOY'])
data = session.execute(bound_stmt)

for each in data:
    print each

#%%    Cassandra CQL object mapper for Python

from cqlengine import connection
from cqlengine.models import Model
from cqlengine.management import sync_table

# Connect to the demo keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "amazon_data")

# Define a model/object similar to the table which is defined in the database
# A model is a Python class that represents a CQL table - subdepts. 
# Here we are defining a model of the subdepts_t1 table,
## with the same columns that exist in our database.

class subdepts_t1(Model):
  depart_name = columns.Text(primary_key=True)
  subdepart_name = columns.Text(primary_key=True)
  product_id  = columns.Text(primary_key=True)            
  product_name = columns.Text()
  product_price = columns.Text()
  week_number = columns.Text()

sync_table(subdepts_t1)

subdepts_t1.create( depart_name ='Amazon Launchpad',subdepart_name = 'Body', product_id = 'B00UFOBDNY', product_name = 'SmartyPants Adult Complete Gummy Vita...', product_price = '$18.95', week_number = 'week1')
data = subdepts_t1.get(depart_name = 'Amazon Launchpad' , subdepart_name = 'Body' , product_id= 'B00UFOBDNY' )

print type(data) 
print data

## Updation
data.update(product_price = "$45")
print data.product_price


#%%   Consistency Level

from cassandra import ConsistencyLevel

query = SimpleStatement( "INSERT INTO subdepts_t1 (depart_name,subdepart_name,product_id, product_name, product_price , week_number ) VALUES( %s, %s, %s, %s, %s, %s)",
    consistency_level=ConsistencyLevel.QUORUM)

#session.execute(query, ('Amazon Launchpad', 'Body','B01LB1O59I',	'SmartyPants Probiotic & Prebiotic Imm...','$67','week2'))

query = SimpleStatement( "INSERT INTO subdepts_t1 (depart_name,subdepart_name,product_id, product_name, product_price , week_number ) VALUES( %s, %s, %s, %s, %s, %s)",
    consistency_level=ConsistencyLevel.ONE)

session.execute(query, ('Amazon Launchpad', 'Body','B01LB1O59I',	'SmartyPants Probiotic & Prebiotic Imm...','$67','week2'))

#%% Conssitency Level with prepared statements

## Preparing the statement
## Setting the consistency level
## Binding the variables
## Executing the statement

print("#################### Insertion ###################### \n")
prepared_stmt = session.prepare ( "insert into subdepts_t1 (depart_name,subdepart_name,product_id, product_name, product_price , week_number ) VALUES( ?, ?, ?, ?, ?, ?) ")
#prepared_stmt.consistency_level = ConsistencyLevel.QUORUM
prepared_stmt.consistency_level = ConsistencyLevel.ONE

bound_stmt = prepared_stmt.bind([ 'Amazon Launchpad',	'Body', 'B00VF67AC4',	'SmartyPants jhgghhgh ', '$43.45','week1'])
stmt = session.execute(bound_stmt)

data = session.execute(""" select * from subdepts_t1 """)
for each in data:
    print each


