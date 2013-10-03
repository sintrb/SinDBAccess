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




