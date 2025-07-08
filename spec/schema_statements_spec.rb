require "spec_helper"

RSpec.describe ActiveRecord::ConnectionAdapters::Athena::SchemaStatements do
  let(:config) do
    {
      adapter: "athena",
      database: "test_database",
      s3_output_location: "s3://test-bucket/query-results/",
      work_group: "primary",
      connection_options: {
        aws_config: {
          region: "us-east-1",
          access_key_id: "test_key",
          secret_access_key: "test_secret"
        }
      }
    }
  end

  let(:adapter) { ActiveRecord::ConnectionAdapters::AthenaAdapter.new(config) }

  describe "#lookup_cast_type_symbol" do
    it "returns correct symbols for different SQL types" do
      expect(adapter.send(:lookup_cast_type_symbol, "string")).to eq(:string)
      expect(adapter.send(:lookup_cast_type_symbol, "varchar")).to eq(:string)
      expect(adapter.send(:lookup_cast_type_symbol, "bigint")).to eq(:integer)
      expect(adapter.send(:lookup_cast_type_symbol, "int")).to eq(:integer)
      expect(adapter.send(:lookup_cast_type_symbol, "double")).to eq(:float)
      expect(adapter.send(:lookup_cast_type_symbol, "float")).to eq(:float)
      expect(adapter.send(:lookup_cast_type_symbol, "decimal")).to eq(:decimal)
      expect(adapter.send(:lookup_cast_type_symbol, "boolean")).to eq(:boolean)
      expect(adapter.send(:lookup_cast_type_symbol, "timestamp")).to eq(:datetime)
      expect(adapter.send(:lookup_cast_type_symbol, "date")).to eq(:date)
      expect(adapter.send(:lookup_cast_type_symbol, "time")).to eq(:time)
      expect(adapter.send(:lookup_cast_type_symbol, "binary")).to eq(:binary)
      expect(adapter.send(:lookup_cast_type_symbol, "unknown_type")).to eq(:string)
    end
  end

  describe "#lookup_cast_type" do
    it "returns correct ActiveRecord types for different SQL types" do
      expect(adapter.send(:lookup_cast_type, "string")).to be_a(ActiveRecord::Type::String)
      expect(adapter.send(:lookup_cast_type, "bigint")).to be_a(ActiveRecord::Type::Integer)
      expect(adapter.send(:lookup_cast_type, "double")).to be_a(ActiveRecord::Type::Float)
      expect(adapter.send(:lookup_cast_type, "decimal")).to be_a(ActiveRecord::Type::Decimal)
      expect(adapter.send(:lookup_cast_type, "boolean")).to be_a(ActiveRecord::Type::Boolean)
      expect(adapter.send(:lookup_cast_type, "timestamp")).to be_a(ActiveRecord::Type::DateTime)
      expect(adapter.send(:lookup_cast_type, "date")).to be_a(ActiveRecord::Type::Date)
      expect(adapter.send(:lookup_cast_type, "time")).to be_a(ActiveRecord::Type::Time)
      expect(adapter.send(:lookup_cast_type, "binary")).to be_a(ActiveRecord::Type::Binary)
    end
  end

  describe "schema modification methods" do
    it "raises NotImplementedError for unsupported operations" do
      expect { adapter.create_table("test") }.to raise_error(NotImplementedError, /CREATE TABLE is not supported/)
      expect { adapter.rename_table("old", "new") }.to raise_error(NotImplementedError, /RENAME TABLE is not supported/)
      expect { adapter.add_column("test", "col", :string) }.to raise_error(NotImplementedError, /ADD COLUMN is not supported/)
      expect { adapter.remove_column("test", "col") }.to raise_error(NotImplementedError, /REMOVE COLUMN is not supported/)
      expect { adapter.change_column("test", "col", :string) }.to raise_error(NotImplementedError, /CHANGE COLUMN is not supported/)
      expect { adapter.rename_column("test", "old", "new") }.to raise_error(NotImplementedError, /RENAME COLUMN is not supported/)
      expect { adapter.add_index("test", "col") }.to raise_error(NotImplementedError, /ADD INDEX is not supported/)
      expect { adapter.remove_index("test", "col") }.to raise_error(NotImplementedError, /REMOVE INDEX is not supported/)
    end
  end

  describe "#indexes" do
    it "returns empty array" do
      expect(adapter.indexes("test_table")).to eq([])
    end
  end

  describe "#primary_key" do
    it "returns nil" do
      expect(adapter.primary_key("test_table")).to be_nil
    end
  end

  describe "#foreign_keys" do
    it "returns empty array" do
      expect(adapter.foreign_keys("test_table")).to eq([])
    end
  end
end