/*
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#include <stdio.h>
#include <stdlib.h>
#include "mysql.h"

static void change_user(MYSQL *sock, const char *user, const char *password,
                        const char *db, my_bool warning)
{
  if (mysql_change_user(sock, user, password, db) != warning)
  {
    printf("Couldn't change user to: user: '%s', password: '%s', db: '%s':  "
           "Error: %s\n", user, password ? password : "", db ? db : "",
            mysql_error(sock));
  }
}

static void test_change_user(MYSQL *sock, const char *user,
                             const char *password)
{
  MYSQL_RES *results;
  MYSQL_ROW record;

  change_user(sock, user, password, NULL, 0);
  mysql_query(sock, "SHOW GRANTS FOR CURRENT_USER;");
  results = mysql_store_result(sock);

  while((record = mysql_fetch_row(results))) {
    printf("%s\n", record[0]);
  }
}

/*
  Run by mysql-test/t/proxy_user.test
*/
int main(int argc, char **argv)
{
  MYSQL *sock_create;
  MYSQL *sock_test;
  MYSQL *sock_destroy;

  if (argc != 2)
  {
    fprintf(stderr,"usage : proxy_user_test <path-to-socket>\n\n");
    exit(1);
  }

  /****************************************************************
     Create the users necessary for the tests
  */
  if (!(sock_create=mysql_init(0)))
  {
    printf("Couldn't initialize mysql struct\n");
    exit(1);
  }
  mysql_options(sock_create, MYSQL_READ_DEFAULT_GROUP, "connect");

  if (!mysql_real_connect(sock_create, NULL, "root", NULL, NULL, 0,
                          argv[1], 0))
  {
    printf("Couldn't connect to engine!\n%s\n", mysql_error(sock_create));
    perror("");
    exit(1);
  }

  if (!mysql_query(sock_create,
                   "CREATE USER 'proxy_user'@'localhost' IDENTIFIED BY 'pass';"))
    printf("CREATE USER 'proxy_user'@'localhost' IDENTIFIED BY 'pass';\n");

  if (!mysql_query(sock_create,
                   "CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'pass2';"))
    printf("CREATE USER 'test_user'@'localhost' IDENTIFIED BY 'pass2';\n");

  if (!mysql_query(sock_create,
                   "CREATE USER 'testabc'@'localhost' IDENTIFIED BY 'foobar';"))
    printf("CREATE USER 'testabc'@'localhost' IDENTIFIED BY 'foobar';\n");

  if (!mysql_query(sock_create,
                   "GRANT ALL ON *.* to 'testabc'@'localhost';"))
    printf("GRANT ALL ON *.* to 'testabc'@'localhost';\n");

  if (!mysql_query(sock_create,
                   "GRANT INSERT ON *.* to 'test_user'@'localhost';"))
    printf("GRANT INSERT ON *.* to 'test_user'@'localhost';\n");

  mysql_close(sock_create);

  /******************************************************************
     Run the change_user tests!
  */

  if (!(sock_test=mysql_init(0)))
  {
    printf("Couldn't initialize mysql struct\n");
    exit(1);
  }
  mysql_options(sock_test, MYSQL_READ_DEFAULT_GROUP, "connect");

  if (!mysql_real_connect(sock_test, NULL, "proxy_user", "pass", NULL, 0,
                          argv[1], 0))
  {
    printf("Couldn't connect to engine!\n%s\n", mysql_error(sock_test));
    perror("");
    exit(1);
  }
  sock_test->reconnect= 1;

  if (mysql_select_db(sock_test, "test"))
  {
    printf("Couldn't select database test: Error: %s\n",
            mysql_error(sock_test));
  }

  // Should succeed, we have proxy_user privs
  test_change_user(sock_test, "test_user", "foo");

  // Should fail, we are not proxy user and give bad password
  test_change_user(sock_test, "testabc", "bababa");

  // Should succeed, we supply correct user and pass
  test_change_user(sock_test, "proxy_user", "pass");

  // Should fail, notauser doesn't exist
  test_change_user(sock_test, "notauser", NULL);

  // Should succeed, we have proxy_user privs again.
  test_change_user(sock_test, "testabc", "bababa");

  mysql_close(sock_test);

  /******************************************************************
     Delete the users we created so that our database does not retain
     bad state
  */

  if (!(sock_destroy=mysql_init(0)))
  {
    printf("Couldn't initialize mysql struct\n");
    exit(1);
  }
  mysql_options(sock_destroy, MYSQL_READ_DEFAULT_GROUP, "connect");

  if (!mysql_real_connect(sock_destroy, NULL, "root", NULL, NULL, 0,
                          argv[1], 0))
  {
    printf("Couldn't connect to engine!\n%s\n", mysql_error(sock_destroy));
    perror("");
    exit(1);
  }

  if (!mysql_query(sock_destroy, "DROP USER 'proxy_user'@'localhost';"))
    printf("DROP USER 'proxy_user'@'localhost';\n");

  if (!mysql_query(sock_destroy, "DROP USER 'test_user'@'localhost';"))
    printf("DROP USER 'test_user'@'localhost';\n");

  if (!mysql_query(sock_destroy, "DROP USER 'testabc'@'localhost';"))
    printf("DROP USER 'testabc'@'localhost';\n");

  mysql_close(sock_destroy);
  exit(0);
  return 0;
}
