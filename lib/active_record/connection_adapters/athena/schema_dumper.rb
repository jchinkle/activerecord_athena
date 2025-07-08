module ActiveRecord
  module ConnectionAdapters
    module Athena
      class SchemaDumper < ConnectionAdapters::SchemaDumper
        private

        def header(stream)
          stream.puts <<~HEADER
            # This file is auto-generated from the current state of the database. Instead
            # of editing this file, please use the migrations feature of Active Record to
            # incrementally modify your database, and then regenerate this schema definition.
            #
            # Note that this schema.rb definition is the authoritative source for your
            # database schema. If you need to create the application database on another
            # system, you should be using db:schema:load, not running all the migrations
            # from scratch. The latter is a flawed and unsustainable approach (the more
            # migrations you'll amass, the slower it'll run and the greater likelihood for
            # issues).
            #
            # It's strongly recommended that you check this file into your version control system.

            ActiveRecord::Schema.define(version: #{ActiveRecord::Migrator.current_version}) do
          HEADER
        end

        def trailer(stream)
          stream.puts "end"
        end

        def table(table, stream)
          columns = @connection.columns(table)
          begin
            tbl = StringIO.new

            # Athena tables are typically external tables
            tbl.print "  create_table #{remove_prefix_and_suffix(table).inspect}"
            tbl.print ", force: :cascade"
            tbl.print ", options: \"EXTERNAL\""
            tbl.puts " do |t|"

            # Athena doesn't have traditional primary keys, so we skip that logic
            columns.each do |column|
              raise StandardError, "Unknown type '#{column.sql_type}' for column '#{column.name}'" unless @connection.valid_type?(column.type)
              next if column.name == "id"
              
              type, colspec = column_spec(column)
              tbl.print "    t.#{type} #{column.name.inspect}"
              tbl.print ", #{format_colspec(colspec)}" if colspec.present?
              tbl.puts
            end

            tbl.puts "  end"
            tbl.puts

            indexes(table, tbl)

            tbl.rewind
            stream.print tbl.read
          rescue => e
            stream.puts "# Could not dump table #{table.inspect} because of following #{e.class}"
            stream.puts "#   #{e.message}"
            stream.puts
          end
        end

        def indexes(table, stream)
          # Athena doesn't support traditional indexes
          # This method is left empty intentionally
        end

        def foreign_keys(table, stream)
          # Athena doesn't support foreign keys
          # This method is left empty intentionally
        end
      end
    end
  end
end