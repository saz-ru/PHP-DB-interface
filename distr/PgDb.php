<?php

/**
 * PostgreeSQL interface
 *
 * Safe and convenient way to handle SQL queries utilizing type-hinted placeholders.
 *
 * Key features
 * - set of helper functions to get the desired result right out of query, like in PEAR::DB
 * - conditional query building using parse() method to build queries of whatever comlexity,
 *   while keeping extra safety of placeholders
 * - type-hinted placeholders
 *
 *  Type-hinted placeholders are great because
 * - safe, as any other [properly implemented] placeholders
 * - no need for manual escaping or binding, makes the code extra DRY
 * - allows support for non-standard types such as identifier or array, which saves A LOT of pain in the back.
 *
 * Supported placeholders at the moment are:
 *
 * ?s ("string")  - strings (also DATE, FLOAT and DECIMAL)
 * ?i ("integer") - the name says it all
 * ?n ("name")    - identifiers (table and field names)
 * ?a ("array")   - complex placeholder for IN() operator  (substituted with string of 'a','b','c' format, without parentesis)
 * ?u ("update")  - complex placeholder for SET operator (substituted with string of field1='value1',field2='value2' format)
 * ?v ("insert")  - complex placeholder for INSERT operator (substituted with string of (field1,field2) VALUES ('value1','value2') format)
 * and
 * ?p ("parsed") - special type placeholder, for inserting already parsed statements without any processing, to avoid double parsing.
 *
 * Connection:
 *
 * $db = new PgDb(); // with default settings
 *
 * $opts = array(
 *    'user'    => 'user',
 *    'pass'    => 'pass',
 *    'db'      => 'db',
 *    'charset' => 'utf-8'
 * );
 * $db = new PgDb($opts); // with some of the default settings overwritten
 *
 * Alternatively, you can just pass an existing postgre instance that will be used to run queries
 * instead of creating a new connection.
 * Excellent choice for migration!
 *
 * $db = new PgDb(['postgre' => $postgre]);
 *
 * Some examples:
 *
 * $name = $db->getOne('SELECT name FROM table WHERE id = ?i',$_GET['id']);
 * $data = $db->getInd('id','SELECT * FROM ?n WHERE id IN ?a','table', array(1,2));
 * $data = $db->getAll("SELECT * FROM ?n WHERE mod=?s LIMIT ?i",$table,$mod,$limit);
 *
 * $ids  = $db->getCol("SELECT id FROM tags WHERE tagname = ?s",$tag);
 * $data = $db->getAll("SELECT * FROM table WHERE category IN (?a)",$ids);
 *
 * $data = array('offers_in' => $in, 'offers_out' => $out);
 * $sql  = "INSERT INTO stats SET pid=?i,dt=CURDATE(),?u ON DUPLICATE KEY UPDATE ?u";
 * $db->query($sql,$pid,$data,$data);
 *
 * $data = array('offers_in' => $in, 'offers_out' => $out);
 * $sql  = "INSERT INTO ?n ?v";
 * $db->query($sql,'table',$data);
 *
 * If you need to retrieving the generated sequence ID:
 * $name_id = $db->query("INSERT INTO ?n ?v RETURNING col_id",'table',$data);
 *
 * if ($var === null) {
 *     $sqlpart = "field is null";
 * } else {
 *     $sqlpart = $db->parse("field = ?s", $var);
 * }
 * $data = $db->getAll("SELECT * FROM table WHERE ?p", $bar, $sqlpart);
 *
 */
include 'PgDbException.php';

class PgDb {
  protected $conn;
  protected $stats;
  protected $emode;
  protected $exname;
  protected $defaults = array(
      'host'      => 'localhost',
      'user'      => 'user',
      'pass'      => 'pass',
      'db'        => 'db_name',
      'port'      => null,
      'pconnect'  => false, // primary connect
      'options'   => '--client_encoding=UTF8', // postgreSQL options
      'errmode'   => 'exception', //or 'error'
      'exception' => 'PgDbException', //Exception class name
  );
  const RESULT_ASSOC = PGSQL_ASSOC;
  const RESULT_NUM = PGSQL_NUM;

  public function __construct(array $opt = array()) {
    $opt = array_merge($this->defaults, $opt);
    $this->emode = $opt['errmode'];
    $this->exname = $opt['exception'];

    $connString = $opt['host'] ? 'host=' . $opt['host'] . ' ' : '';
    $connString .= $opt['port'] ? 'port=' . $opt['port'] . ' ' : '';
    $connString .= $opt['options'] ? 'options=' . $opt['options'] . ' ' : '';
    $connString .= $opt['db'] ? 'dbname=' . $opt['db'] . ' ' : '';
    $connString .= $opt['user'] ? 'user=' . $opt['user'] . ' ' : '';
    $connString .= $opt['pass'] ? 'password=' . $opt['pass'] . ' ' : '';

    if ($opt['pconnect']) {
      @$this->conn = pg_pconnect($connString);
    } else {
      @$this->conn = pg_connect($connString);
    }
    if (!$this->conn) {
      $this->error(pg_last_error() . " " . pg_last_error());
    }
    unset($opt); // I am paranoid
  }

  /**
   * Conventional function to run a query with placeholders. A pg_query wrapper with placeholders support
   *
   * Examples:
   * $db->query("DELETE FROM table WHERE id=?i", $id);
   *
   * @param string $query - an SQL query with placeholders
   * @param mixed $arg,... unlimited number of arguments to match placeholders in the query
   * @return false|resource
   */
  public function query() {
    return $this->rawQuery($this->prepareQuery(func_get_args()));
  }

  /**
   * Conventional function to fetch single row.
   *
   * @param resource $result - pg result
   * @param int $mode - optional fetch mode, RESULT_ASSOC|RESULT_NUM, default RESULT_ASSOC
   * @return array|false whatever pg_fetch_array returns
   */
  public function fetch($result, $mode = self::RESULT_ASSOC) {
    return pg_fetch_array($result,null, $mode);
  }

  /**
   * Conventional function to get number of affected rows.
   *
   * @param resource $result - pg result
   * @return int whatever pg_affected_rows returns
   */
  public function affectedRows($result) {
    return pg_affected_rows($result);
  }

  /**
   * Conventional function to get number of rows in the resultset.
   *
   * @param resource $result - pg result
   * @return int whatever pg_num_rows returns
   */
  public function numRows($result) {
    return pg_num_rows($result);
  }

  /**
   * Conventional function to free the resultset.
   * @param resource $result - postgre result
   */
  public function free($result) {
    pg_free_result($result);
  }

  /**
   * Helper function to get scalar value right out of query and optional arguments
   *
   * Examples:
   * $name = $db->getOne("SELECT name FROM table WHERE id=1");
   * $name = $db->getOne("SELECT name FROM table WHERE id=?i", $id);
   *
   * @param string $query - an SQL query with placeholders
   * @param mixed $arg,... unlimited number of arguments to match placeholders in the query
   * @return string|false either first column of the first row of resultset or false if none found
   */
  public function getOne() {
    $query = $this->prepareQuery(func_get_args());
    if ($res = $this->rawQuery($query)) {
      $row = $this->fetch($res);
      if (is_array($row)) {
        return reset($row);
      }
      $this->free($res);
    }
    return false;
  }

  /**
   * Helper function to get single row right out of query and optional arguments
   *
   * Examples:
   * $data = $db->getRow("SELECT * FROM table WHERE id=1");
   * $data = $db->getOne("SELECT * FROM table WHERE id=?i", $id);
   *
   * @param string $query - an SQL query with placeholders
   * @param mixed $arg,... unlimited number of arguments to match placeholders in the query
   * @return array|false either associative array contains first row of resultset or false if none found
   */
  public function getRow() {
    $query = $this->prepareQuery(func_get_args());
    if ($res = $this->rawQuery($query)) {
      $ret = $this->fetch($res);
      $this->free($res);
      return $ret;
    }
    return false;
  }

  /**
   * Helper function to get single column right out of query and optional arguments
   *
   * Examples:
   * $ids = $db->getCol("SELECT id FROM table WHERE cat=1");
   * $ids = $db->getCol("SELECT id FROM tags WHERE tagname = ?s", $tag);
   *
   * @param string $query - an SQL query with placeholders
   * @param mixed $arg,... unlimited number of arguments to match placeholders in the query
   * @return array|false either enumerated array of first fields of all rows of resultset or false if none found
   */
  public function getCol() {
    $ret = array();
    $query = $this->prepareQuery(func_get_args());
    if ($res = $this->rawQuery($query)) {
      while ($row = $this->fetch($res)) {
        $ret[] = reset($row);
      }
      $this->free($res);
    }
    return $ret;
  }

  /**
   * Helper function to get all the rows of resultset right out of query and optional arguments
   *
   * Examples:
   * $data = $db->getAll("SELECT * FROM table");
   * $data = $db->getAll("SELECT * FROM table LIMIT ?i,?i", $start, $rows);
   *
   * @param string $query - an SQL query with placeholders
   * @param mixed $arg,... unlimited number of arguments to match placeholders in the query
   * @return array enumerated 2d array contains the resultset. Empty if no rows found.
   */
  public function getAll() {
    $ret = array();
    $query = $this->prepareQuery(func_get_args());
    if ($res = $this->rawQuery($query)) {
      while ($row = $this->fetch($res)) {
        $ret[] = $row;
      }
      $this->free($res);
    }
    return $ret;
  }

  /**
   * Helper function to get all the rows of resultSet into indexed array right out of query and optional arguments
   *
   * Examples:
   * $data = $db->getInd("id", "SELECT * FROM table");
   * $data = $db->getInd("id", "SELECT * FROM table LIMIT ?i,?i", $start, $rows);
   *
   * @param string $index - name of the field which value is used to index resulting array
   * @param string $query - an SQL query with placeholders
   * @param mixed $arg,... unlimited number of arguments to match placeholders in the query
   * @return array - associative 2d array contains the resultset. Empty if no rows found.
   */
  public function getInd() {
    $args = func_get_args();
    $index = array_shift($args);
    $query = $this->prepareQuery($args);
    $ret = array();
    if ($res = $this->rawQuery($query)) {
      while ($row = $this->fetch($res)) {
        $ret[$row[$index]] = $row;
      }
      $this->free($res);
    }
    return $ret;
  }

  /**
   * Helper function to get a dictionary-style array right out of query and optional arguments
   *
   * Examples:
   * $data = $db->getIndCol("name", "SELECT name, id FROM cities");
   *
   * @param string $index - name of the field which value is used to index resulting array
   * @param string $query - an SQL query with placeholders
   * @param mixed $arg,... unlimited number of arguments to match placeholders in the query
   * @return array - associative array contains key=value pairs out of resultset. Empty if no rows found.
   */
  public function getIndCol() {
    $args = func_get_args();
    $index = array_shift($args);
    $query = $this->prepareQuery($args);
    $ret = array();
    if ($res = $this->rawQuery($query)) {
      while ($row = $this->fetch($res)) {
        $key = $row[$index];
        unset($row[$index]);
        $ret[$key] = reset($row);
      }
      $this->free($res);
    }
    return $ret;
  }

  /**
   * Function to parse placeholders either in the full query or a query part
   * unlike native prepared statements, allows ANY query part to be parsed
   *
   * useful for debug
   * and EXTREMELY useful for conditional query building
   * like adding various query parts using loops, conditions, etc.
   * already parsed parts have to be added via ?p placeholder
   *
   * Examples:
   * $query = $db->parse("SELECT * FROM table WHERE foo=?s AND bar=?s", $foo, $bar);
   * echo $query;
   *
   * if ($foo) {
   *     $qpart = $db->parse(" AND foo=?s", $foo);
   * }
   * $data = $db->getAll("SELECT * FROM table WHERE bar=?s ?p", $bar, $qpart);
   *
   * @param string $query - whatever expression contains placeholders
   * @param mixed $arg,... unlimited number of arguments to match placeholders in the expression
   * @return string - initial expression with placeholders substituted with data.
   */
  public function parse() {
    return $this->prepareQuery(func_get_args());
  }

  /**
   * function to implement whitelisting feature
   * sometimes we can't allow a non-validated user-supplied data to the query even through placeholder
   * especially if it comes down to SQL OPERATORS
   *
   * Example:
   *
   * $order = $db->whiteList($_GET['order'], array('name','price'));
   * $dir   = $db->whiteList($_GET['dir'],   array('ASC','DESC'));
   * if (!$order || !dir) {
   *     throw new http404(); //non-expected values should cause 404 or similar response
   * }
   * $sql  = "SELECT * FROM table ORDER BY ?p ?p LIMIT ?i,?i"
   * $data = $db->getArr($sql, $order, $dir, $start, $per_page);
   *
   * @param string $input - field name to test
   * @param array $allowed - an array with allowed variants
   * @param bool|string $default - optional variable to set if no match found. Default to false.
   * @return false|string - either sanitized value or false
   */
  public function whiteList($input, $allowed, $default = false) {
    $found = array_search($input, $allowed,false);
    return ($found === false) ? $default : $allowed[$found];
  }

  /**
   * function to filter out arrays, for the whitelisting purposes
   * useful to pass entire superGlobal to the INSERT or UPDATE query
   * OUGHT to be used for this purpose,
   * as there could be fields to which user should have no access to.
   *
   * Example:
   * $allowed = array('title','url','body','rating','term','type');
   * $data    = $db->filterArray($_POST,$allowed);
   * $sql     = "INSERT INTO ?n SET ?u";
   * $db->query($sql,$table,$data);
   *
   * @param  array $input - source array
   * @param  array $allowed - an array with allowed field names
   * @return array filtered out source array
   */
  public function filterArray($input, $allowed) {
    foreach (array_keys($input) as $key) {
      if (!in_array($key, $allowed, false)) {
        unset($input[$key]);
      }
    }
    return $input;
  }

  /**
   * Function to get last executed query.
   *
   * @return string|null either last executed query or null if were none
   */
  public function lastQuery() {
    $last = end($this->stats);
    return $last['query'];
  }

  /**
   * Function to get all query statistics.
   *
   * @return array contains all executed queries with timings and errors
   */
  public function getStats() {
    return $this->stats;
  }

  /**
   * protected function which actually runs a query against Mysql server.
   * also logs some stats like profiling info and error message
   *
   * @param string $query - a regular SQL query
   * @return resource|false result resource or false on error
   */
  protected function rawQuery($query) {
    $start = microtime(TRUE);
    $res = pg_query($this->conn, $query);
    $timer = microtime(TRUE) - $start;
    $this->stats[] = array(
        'query' => $query,
        'start' => $start,
        'timer' => $timer,
    );
    if (!$res) {
      $error = pg_last_error($this->conn);
      end($this->stats);
      $key = key($this->stats);
      $this->stats[$key]['error'] = $error;      
      $this->cutStats();
      $this->error("$error. Full query: [$query]");
    } elseif (!$this->numRows($res)) {
      $this->free($res);
      return false;
    }
    $this->cutStats();
    return $res;
  }

  /**
   * Description:
   * ?n - name
   * ?s - string
   * ?i - integer|float|decimal
   * ?u - split assoc array to string: 'key1' = 'value1', 'key2' = 'value2'
   * ?a - implode indexed array to comma list: value1,value2,...
   * ?p - not escaped string
   * ?v - split assoc array to string: (key1,key2) VALUES (value1,value2)
   *
   * @param string - query
   * @return string - query
   */
  protected function prepareQuery($args) {
    $query = '';
    $raw = array_shift($args);
    $array = preg_split('~(\?[nsiuapv])~u', strtolower($raw), null, PREG_SPLIT_DELIM_CAPTURE);
    $anum = count($args);
    $pnum = floor(count($array) / 2);
    if ($pnum != $anum) {
      $this->error("Number of args ($anum) doesn't match number of placeholders ($pnum) in [$raw]");
    }
    foreach ($array as $i => $part) {
      if (($i % 2) == 0) {
        $query .= $part;
        continue;
      }
      $value = array_shift($args);
      switch ($part) {
        case '?n':
          $part = $this->escapeIdent($value);
          break;
        case '?s':
          $part = $this->escapeString($value);
          break;
        case '?i':
          $part = $this->escapeInt($value);
          break;
        case '?a':
          $part = $this->createIN($value);
          break;
        case '?u':
          $part = $this->createSET($value);
          break;
        case '?v':
          $part = $this->createINSERT($value);
          break;
        case '?p':
          $part = $value;
          break;
      }
      $query .= $part;
    }
    return $query;
  }

  public function escapeInt($value) {
    if ($value === null) {
      return 'null';
    }
    if (!is_numeric($value)) {
      $this->error("Integer (?i) placeholder expects numeric value, " . gettype($value) . " given");
      return false;
    }
    if (is_float($value)) {
      $value = number_format($value, 0, '.', ''); // may lose precision on big numbers
    }
    return $value;
  }

  public function escapeString($value) {
    if (empty($value)) {
      return 'null';
    }
    return "'" . pg_escape_string($this->conn, $value) . "'";
  }

  protected function escapeIdent($value) {
    if ($value) {
      return trim(str_replace("`", "", $value));
    } else {
      $this->error("Empty value for identifier (?n) placeholder");
      return null;
    }
  }

  protected function createIN($data) {
    if (!is_array($data)) {
      $this->error("Value for IN (?a) placeholder should be array");
      return null;
    }
    if (!$data) {
      return 'null';
    }
    $query = $comma = '';
    foreach ($data as $value) {
      $query .= $comma . $this->escapeString($value);
      $comma = ",";
    }
    return $query;
  }

  protected function createINSERT($data) {
    if (!is_array($data)) {
      $this->error("SET (?u) placeholder expects array, " . gettype($data) . " given");
      return null;
    }
    if (!$data) {
      $this->error("Empty array for SET (?u) placeholder");
      return null;
    }
    $keys = $comma = '';
    foreach (array_keys($data) as $key) {
      $keys .= $comma . $this->escapeIdent($key);
      $comma = ",";
    }
    $values = $comma = '';
    foreach (array_values($data) as $value) {
      $values .= $comma . $this->escapeString($value);
      $comma = ",";
    }
    return ' ('.$keys.') VALUES ('.$values.') ';
  }

  protected function createSET($data) {
    if (!is_array($data)) {
      $this->error("SET (?u) placeholder expects array, " . gettype($data) . " given");
      return null;
    }
    if (!$data) {
      $this->error("Empty array for SET (?u) placeholder");
      return null;
    }
    $query = $comma = '';
    foreach ($data as $key => $value) {
      $query .= $comma . $this->escapeIdent($key) . '=' . $this->escapeString($value);
      $comma = ",";
    }
    return $query;
  }

  protected function error($err) {
    $err = __CLASS__ . ": " . $err;
    if ($this->emode == 'error') {
      $err .= ". Error initiated in " . $this->caller() . ", thrown";
      trigger_error($err, E_USER_ERROR);
    } else {
      throw new $this->exname($err);
    }
  }

  protected function caller() {
    $trace = debug_backtrace();
    $caller = '';
    foreach ($trace as $t) {
      if (isset($t['class']) && $t['class'] == __CLASS__) {
        $caller = $t['file'] . " on line " . $t['line'];
      } else {
        break;
      }
    }
    return $caller;
  }

  /**
   * On a long run we can eat up too much memory with more statistics
   * Let's keep it at reasonable size, leaving only last 100 entries.
   */
  protected function cutStats() {
    if (count($this->stats) > 100) {
      reset($this->stats);
      $first = key($this->stats);
      unset($this->stats[$first]);
    }
  }
}
