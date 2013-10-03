#SinDBAccess

##About
A Python Database Access Helper.

##How to use

You can use SinDBAccess like this follow:

* Create a MySQL connection.
<pre><code> con = MySQLdb.connect(host='127.0.0.1', user='username', passwd='passwd', db='dbname', port=3306)</code></pre>

* Then use this connection to create a SinDBAccess
<pre><code>dba = SinDBAccess(con)</code></pre>

* After this, you can use this <code>dba</code> to access Database.
* At most time, you need create a table using a template.
<pre><code># Create a table, this table has 3 fields(id, name and age).
# id is int type type
# name is char(32) type
# age is int type
dba.create_table(table, {'name':'char(32)','age':0}, new=True)</code></pre>



