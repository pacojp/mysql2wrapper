# -*- coding: utf-8 -*-

require 'yaml'
require 'mysql2'

class Mysql2wrapper::Client
  attr_accessor :config, :client, :logger
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
      self.logger.info "mysql2 client created with #{config.inspect}"
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

  def query(str,color=QUERY_BASE_COLOR)
    res = self.class.query(self.client,str,self.logger,color)
    @affected_rows = self.client.affected_rows
    res
  end

  def transaction(&proc)
    raise ArgumentError, "No block was given" unless block_given?
    #query "SET AUTOCOMMIT=0;",QUERY_SPECIAL_COLOR
    query "BEGIN",QUERY_SPECIAL_COLOR
    begin
      yield
      query "COMMIT",QUERY_SPECIAL_COLOR
    rescue => e
      query "ROLLBACK",QUERY_SPECIAL_COLOR
      raise e
    end
  end

  def self.make_config_key_symbol(config)
    new_config = {}
    config.each do |key,value|
      new_config[key.to_sym] = value
    end
    config = new_config
  end

  def count(table_name,key_name='*')
    query("SELECT COUNT(#{escape(key_name)}) AS cnt FROM #{escape(table_name)}").first['cnt']
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
    when String
      where = "WHERE #{where}"
    when UpdateAllClass.class
      where = ''
    else
      raise ArgumentError, "where must be String or UpdateAll"
    end
    query = "UPDATE `#{escape(table_name)}` SET #{
      hash.map do |key,value|
        "`#{escape(key.to_s)}` = #{proceed_value(value)}"
      end.join(',')
    }" + where
    self.query(query)
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
    else
      s = value
      s = s.to_s unless s.kind_of?(String)
      if s.respond_to?(:function_sql?) && s.function_sql?
        "#{value.to_s}"
      else
        "'#{escape(value.to_s)}'"
      end
    end
  end

  def insert(table_name,data,multiple_insert_by=MULTIPLE_INSERT_DEFAULT)
    @affected_rows = 0 # 一応リセットしとくか、、、
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

    # TODO affected_rows by multiple
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
    db_config = YAML.load_file(yml_path)[environment]
    if db_server_name
      db_config = self.make_config_key_symbol(db_config[db_server_name])
    else
      db_config = self.make_config_key_symbol(db_config)
    end
    db_config
  end

  def tables
    query = "select * from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '#{escape(self.config[:database])}' AND TABLE_TYPE = 'BASE TABLE'"
    self.client.query query
  end
end
