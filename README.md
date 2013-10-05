#SinDBAccess

##About
A Python Database Access Helper.

##How to use

You can use SinDBAccess like this follow:

* Create a MySQL connection.
<pre><code> con = MySQLdb.connect(host='127.0.0.1', user='username', passwd='passwd', db='dbname', port=3306)</code></pre>

* Then use this connection to create a SinDBAccess
<pre><code>dba = SinDBAccess(con)</code></pre>

* Now, you can use this <code>dba</code> to access Database.
* At most time, you need create a table using a template.
<pre><code># Create a table, this table has 3 fields(id, name and age).
# id is int type type
# name is char(32) type
# age is int type
dba.create_table('tb_user', {'name':'char(32)','age':0}, new=True)</code></pre>

* Then dba will creata a table named 'tb_user' in Database. After this, you can use <code>dba</code> to insert new record to 'tb_user'.
<pre><code>dba.add_object('tb_user', {'name':'robin', 'age':23})</code></pre>

* Of course, you can get record from 'tb_user'.
<pre><code># get all user
users = dba.get_objects('tb_user')

# get all user witch age is 23
users = dba.get_objects('tb_user', columns=['name'], conditions={'age':23})

# get record witch id is 10
user = dba.get_object('tb_user', 10)</code></pre>


