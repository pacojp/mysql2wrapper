# -*- coding: utf-8 -*-

require 'yaml'
require 'mysql2'

class Mysql2wrapper::Client
  attr_accessor :config, :client, :logger, :last_query
  attr_reader :affected_rows

  # 全行更新用のフラグ表現クラス
  class UpdateAllClass;end

  UPDATE_ALL          = UpdateAllClass
  QUERY_BASE_COLOR    = 35
  QUERY_SPECIAL_COLOR = 31

  MULTIPLE_INSERT_DEFAULT = 100

  def initialize(config,_logger=Logger.new(STDOUT))
    self.logger = _logger
    if self.logger
      # TODO パスワードの隠し
      config_c = config.clone
      if config_c[:password].present?
        config_c[:password] = '****'
      end
      self.logger.info "mysql2 client created with #{config_c.inspect}"
    end
    self.client = Mysql2::Client.new(config)
    self.config = config
    # サーバが古いので一応問題あるけど以下の方向で
    # http://kennyqi.com/archives/61.html
    self.class.query(self.client,"SET NAMES 'utf8'",self.logger)
    self.class.query(self.client,"SET SQL_AUTO_IS_NULL=0",self.logger)
    self.class.query(self.client,"SET SQL_MODE=STRICT_ALL_TABLES",self.logger)
  end

  def self.query(client,sql_query,logger=nil,color=QUERY_BASE_COLOR)
    s = Time.now
    ret = client.query(sql_query)
    e = Time.now
    if logger
      # TODO too long sql shorten
      if sql_query.size > 1400
        sql_query = sql_query[0..600] + "\n<< trimed >>\n" + sql_query[(sql_query.size - 600)..(sql_query.size - 1)]
      end
      logger.info "[QUERY] "" \e[#{color}m (#{((e-s)*1000).round(2)}ms/#{client.affected_rows}rows) #{sql_query}\e[0m"
    end
    ret
  end

  def stop_logging
    self.logger = nil
  end

  def dump
    "#{self.config.inspect}\n#{self.client.pretty_inspect}"
  end

  def query(str_query,color=QUERY_BASE_COLOR)
    begin
      case str_query
      when /^SET /,'COMMIT','ROLLBACK'
      else
        self.last_query = str_query
      end
    rescue ArgumentError => e
      # バイナリが絡むとstr_queryに正規表現とかかけられない
      # invalid sequence エラーになる
      self.last_query = str_query
    end
    res = self.class.query(self.client,str_query,self.logger,color)
    @affected_rows = self.client.affected_rows
    res
  end

  # HACKME
  # トランザクションのネストをどう考えるか
  # このライブラリを使うシチュエーションってのは
  # トランザクションのネストを許さない場合ってな気がするので
  # 一応エラーを上げるようにしてますが、、、、
  def transaction(&proc)
    raise ArgumentError, "No block was given" unless block_given?
    #query "SET AUTOCOMMIT=0;",QUERY_SPECIAL_COLOR
    if @__inside_transaction
      # HACHME エラーの種別の検討
      raise StandardError, 'can not nest transaction!!!!'
    end
    @__inside_transaction = true
    query "BEGIN",QUERY_SPECIAL_COLOR
    begin
      yield
      query "COMMIT",QUERY_SPECIAL_COLOR
    rescue => e
      query "ROLLBACK",QUERY_SPECIAL_COLOR
      raise e
    ensure
      @__inside_transaction = false
    end
  end

  def self.make_config_key_symbol(config)
    new_config = {}
    config.each do |key,value|
      new_config[key.to_sym] = value
    end
    config = new_config
  end

  def count(table_name,where=nil,key_name='*')
    query = "SELECT COUNT(#{escape(key_name)}) AS cnt FROM #{escape(table_name)}"
    if where
      query = "#{query} #{parse_where where}"
    end
    query(query).first['cnt']
  end

  def close
    self.client.close if self.client
  end

  def escape(str)
    self.client.escape(str)
  end

  def sanitize(str)
    escape(str)
  end

  #
  # where は 'WHERE'を付けずに指定
  # 全行updateを使用するシチュエーションはほぼ0に
  # 近いと思われるので、簡単には実行できなくしてます
  # 実際に実行する際はwhereにUpdateAllを引数とします
  # client.update 'test01',{:v_int1=>3},Mysql2wrapper::Client::UPDATE_ALL
  #
  def update(table_name,hash,where)
    case where
    when '',nil
      raise ArgumentError, 'can not set blank or nil on where with update(you shoule use UpdateAllClass with where)'
    when UpdateAllClass.class
      where = nil
    when String,Hash
    else
      raise ArgumentError, "where must be String or UpdateAll"
    end
    query = "UPDATE `#{escape(table_name)}` SET #{
      hash.map do |key,value|
        "`#{escape(key.to_s)}` = #{proceed_value(value)}"
      end.join(',')
    }"
    if where
      query = "#{query} #{parse_where where}"
    end
    self.query(query)
  end

  def select(table_name,select,where=nil)
    query = "SELECT #{select} FROM `#{escape table_name}`"
    if where
      query = "#{query} #{parse_where(where)}"
    end
    query query
  end

  def parse_where(v)
    case v
    when String
      if v.size > 0
        "WHERE #{v}"
      else
        ''
      end
    when Hash
      "WHERE #{
        v.map do |key,value|
          case value
          when nil
            "`#{escape(key.to_s)}` IS NULL"
          when Array
            # ここ、、、自動で条件を抜くってのもできるけど、、、、まぁそれで意図しない結果になるより
            # エラーを上げるほうが妥当だろうて
            raise "at least one value needs for #{key.to_s} (can not call in statement with no value)" if value.size == 0
            "`#{escape(key.to_s)}` in (#{value.map{|o|proceed_value(o)}.join(',')})"
          else
            "`#{escape(key.to_s)}` = #{proceed_value(value)}"
          end
        end.join(' AND ')
      }"
    else
      raise 'can set String or Hash on where'
    end
  end

  def update_all_flag
    self.class::UPDATE_ALL
  end

  def proceed_value(value)
    case value
    when Proc
      "'#{escape(value.call.to_s)}'"
    when nil
      "NULL"
    when TrueClass,FalseClass
      if value
        "'1'"
      else
        "'0'"
      end
    when Time,DateTime
      "'#{value.strftime("%Y-%m-%d %H:%M:%S")}'"
    when Date
      "'#{value.strftime("%Y-%m-%d")}'"
    when String
      if value.respond_to?(:function_sql?) && value.function_sql?
        "#{value.to_s}"
      else
        value = escape(value)
        #value = value.encode('utf-8', {:invalid => :replace, :undef => :replace})
        "'#{value}'"
      end
    else
      "'#{escape(value.to_s)}'"
    end
  end

  def insert(table_name,data,multiple_insert_by=MULTIPLE_INSERT_DEFAULT)
    @affected_rows = 0 # 一応リセット
    affected_rows_total = 0
    query = "INSERT INTO `#{escape(table_name)}`"
    _datas = data.clone
    case _datas
    when Array
      ;
    when Hash
      _datas = [_datas]
    else
      raise ArgumentError, "data must be Array or Hash"
    end

    return nil if _datas.size == 0

    _datas.each_slice(multiple_insert_by).each do |rows|
      query = <<"EOS"
INSERT INTO `#{escape(table_name)}`
(#{rows.first.keys.map{|o|"`#{escape(o.to_s)}`"}.join(',')})
VALUES
#{
  rows.map do |row|
  "(#{
    row.map do |key,value|
      proceed_value(value)
    end.join(',')
  })"
  end.join(',')
}
EOS

      self.query(query.chomp)
      affected_rows_total += self.client.affected_rows
    end
    @affected_rows = affected_rows_total
  end

  #
  # db_server_nameはDB名そのもの（複数DB対応）
  #
  def self.config_from_yml(yml_path,environment,db_server_name=nil)
    raise "yaml not found(#{yml_path})" unless File.exists?(yml_path)
    db_config = YAML.load_file(yml_path)[environment]
    if db_server_name
      db_config = db_config[db_server_name]
    end
    unless db_config
      raise "can not get db_config with env:#{environment}#{db_server_name ? "/db_server:#{db_server_name}":''}"
    end
    self.make_config_key_symbol(db_config)
  end

  def tables
    table_informations.map{|o|o['TABLE_NAME']}
  end

  def table_names
    table_informations.map{|o|o['TABLE_NAME']}
  end

  def databases
    query = 'show databases'
    self.client.query(query).map{|ar|ar['Database']}
  end

  def table_informations
    query = "select * from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '#{escape(self.config[:database])}' AND TABLE_TYPE = 'BASE TABLE'"
    tables = self.client.query(query).to_a
    tables.each do |table|
      table['COLUMNS'] = table_information_schema('COLUMNS',table['TABLE_NAME'])
      table['INDEXES'] = table_information_schema('STATISTICS',table['TABLE_NAME'])
      query = "SHOW CREATE TABLE `#{escape(table['TABLE_NAME'])}`"
      table['CREATE TABLE'] = self.client.query(query).first['Create Table']
    end
    tables
  end

  def table_information_schema(type,table_name)
    query = "
SELECT
  *
FROM
  INFORMATION_SCHEMA.#{type}
WHERE
table_name = '#{escape(table_name)}' AND
table_schema = '#{escape(self.config[:database])}'
"
      query(query).to_a
  end
end
