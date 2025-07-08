require "active_record/connection_adapters/abstract_adapter"
require "aws-sdk-athena"
require "aws-sdk-s3"
require "active_record/connection_adapters/athena/schema_statements"
require "active_record/connection_adapters/athena/database_tasks"

module ActiveRecord
  module ConnectionAdapters
    class AthenaAdapter < AbstractAdapter
      include Athena::SchemaStatements
      ADAPTER_NAME = "Athena"

      def initialize(config)
        super(config)
        @connection_options = config[:connection_options] || {}
        @database = config[:database]
        @s3_output_location = config[:s3_output_location]
        @work_group = config[:work_group] || "primary"
      end

      def adapter_name
        ADAPTER_NAME
      end

      def active?
        true
      end

      def reconnect!
        @athena_client = nil
        @s3_client = nil
      end

      def disconnect!
        # Athena doesn't maintain persistent connections
      end

      def supports_migrations?
        false
      end

      def supports_primary_key?
        false
      end

      def supports_bulk_alter?
        false
      end

      def supports_foreign_keys?
        false
      end

      def supports_views?
        true
      end

      def supports_datetime_with_precision?
        true
      end

      def supports_json?
        true
      end

      def native_database_types
        {
          primary_key: "string",
          string: { name: "string" },
          text: { name: "string" },
          integer: { name: "bigint" },
          bigint: { name: "bigint" },
          float: { name: "double" },
          decimal: { name: "decimal" },
          datetime: { name: "timestamp" },
          time: { name: "time" },
          date: { name: "date" },
          binary: { name: "binary" },
          boolean: { name: "boolean" },
          json: { name: "string" }
        }
      end

      def quote_column_name(name)
        "\"#{name}\""
      end

      def quote_table_name(name)
        "\"#{name}\""
      end

      def quoted_true
        "true"
      end

      def quoted_false
        "false"
      end

      def execute(sql, name = nil)
        log(sql, name) do
          execute_query(sql)
        end
      end

      def exec_query(sql, name = "SQL", binds = [], **kwargs)
        log(sql, name) do
          # Replace parameter placeholders with actual values
          prepared_sql = substitute_binds(sql, binds)
          result = execute_query(prepared_sql)
          
          if result[:rows].any?
            columns = result[:column_info].map { |col| col[:name] }
            raw_rows = result[:rows].map { |row| row[:data].map { |cell| cell[:var_char_value] } }
            
            # Filter out header row if it matches column names
            # The first row is often the header row in Athena results
            data_rows = raw_rows
            if raw_rows.first && raw_rows.first == columns
              data_rows = raw_rows.drop(1)
            end
            
            ActiveRecord::Result.new(columns, data_rows)
          else
            ActiveRecord::Result.new([], [])
          end
        end
      end

      def select_all(arel, name = nil, binds = [], **kwargs)
        sql = to_sql(arel, binds)
        exec_query(sql, name, binds)
      end

      private

      def athena_client
        @athena_client ||= Aws::Athena::Client.new(aws_config)
      end

      def s3_client
        @s3_client ||= Aws::S3::Client.new(aws_config)
      end

      def aws_config
        @connection_options[:aws_config] || {}
      end

      def substitute_binds(sql, binds)
        # Handle special case where we have ? placeholders but no binds
        # This often happens with LIMIT clauses in newer ActiveRecord versions
        if binds.empty? && sql.include?('?')
          # For LIMIT clauses, we'll use a reasonable default
          # In production, this might need more sophisticated handling
          sql = sql.gsub(/LIMIT \?/, 'LIMIT 1000')
          return sql
        end
        
        return sql if binds.empty?
        
        # Replace ? placeholders with actual values
        bind_index = 0
        sql.gsub('?') do
          if bind_index < binds.length
            bind = binds[bind_index]
            bind_index += 1
            
            # Handle different types of bind values
            value = bind.respond_to?(:value) ? bind.value : bind
            quote(value)
          else
            '?'
          end
        end
      end

      def quote(value)
        case value
        when String
          "'#{value.gsub("'", "''")}'"
        when Integer, Float
          value.to_s
        when true
          'true'
        when false
          'false'
        when nil
          'NULL'
        when Date
          "'#{value.strftime('%Y-%m-%d')}'"
        when Time, DateTime
          "'#{value.strftime('%Y-%m-%d %H:%M:%S')}'"
        else
          "'#{value.to_s.gsub("'", "''")}'"
        end
      end

      def execute_query(sql)
        query_execution_id = start_query_execution(sql)
        wait_for_query_completion(query_execution_id)
        get_query_results(query_execution_id)
      end

      def start_query_execution(sql)
        response = athena_client.start_query_execution({
          query_string: sql,
          query_execution_context: {
            database: @database
          },
          result_configuration: {
            output_location: @s3_output_location
          },
          work_group: @work_group
        })
        
        response.query_execution_id
      end

      def wait_for_query_completion(query_execution_id)
        loop do
          response = athena_client.get_query_execution({
            query_execution_id: query_execution_id
          })
          
          status = response.query_execution.status.state
          
          case status
          when "SUCCEEDED"
            break
          when "FAILED", "CANCELLED"
            raise ActiveRecord::StatementInvalid, "Query failed: #{response.query_execution.status.state_change_reason}"
          else
            sleep(0.5)
          end
        end
      end

      def get_query_results(query_execution_id)
        response = athena_client.get_query_results({
          query_execution_id: query_execution_id
        })
        
        {
          column_info: response.result_set.result_set_metadata.column_info,
          rows: response.result_set.rows
        }
      end

      def to_sql(arel, binds)
        if arel.respond_to?(:to_sql)
          arel.to_sql
        else
          arel
        end
      end
    end
  end
end

# Register the adapter
ActiveRecord::ConnectionAdapters.register("athena", "ActiveRecord::ConnectionAdapters::AthenaAdapter", "active_record/connection_adapters/athena_adapter")