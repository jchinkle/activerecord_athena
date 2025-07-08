require "spec_helper"

RSpec.describe ActiveRecord::ConnectionAdapters::AthenaAdapter do
  let(:config) do
    {
      adapter: "athena",
      database: "test_database",
      s3_output_location: "s3://test-bucket/query-results/",
      work_group: "primary"
    }
  end

  let(:connection_options) do
    {
      aws_config: {
        region: "us-east-1",
        access_key_id: "test_key",
        secret_access_key: "test_secret"
      }
    }
  end

  subject(:adapter) do
    described_class.new(nil, nil, connection_options, config)
  end

  describe "#adapter_name" do
    it "returns 'Athena'" do
      expect(adapter.adapter_name).to eq("Athena")
    end
  end

  describe "#active?" do
    it "returns true" do
      expect(adapter.active?).to be true
    end
  end

  describe "#supports_migrations?" do
    it "returns false" do
      expect(adapter.supports_migrations?).to be false
    end
  end

  describe "#supports_primary_key?" do
    it "returns false" do
      expect(adapter.supports_primary_key?).to be false
    end
  end

  describe "#supports_foreign_keys?" do
    it "returns false" do
      expect(adapter.supports_foreign_keys?).to be false
    end
  end

  describe "#supports_views?" do
    it "returns true" do
      expect(adapter.supports_views?).to be true
    end
  end

  describe "#native_database_types" do
    it "returns Athena-specific type mappings" do
      types = adapter.native_database_types
      expect(types[:string]).to eq({ name: "string" })
      expect(types[:integer]).to eq({ name: "bigint" })
      expect(types[:float]).to eq({ name: "double" })
      expect(types[:boolean]).to eq({ name: "boolean" })
    end
  end

  describe "#quote_column_name" do
    it "wraps column names in backticks" do
      expect(adapter.quote_column_name("test_column")).to eq("`test_column`")
    end
  end

  describe "#quote_table_name" do
    it "wraps table names in backticks" do
      expect(adapter.quote_table_name("test_table")).to eq("`test_table`")
    end
  end

  describe "#quoted_true" do
    it "returns 'true'" do
      expect(adapter.quoted_true).to eq("true")
    end
  end

  describe "#quoted_false" do
    it "returns 'false'" do
      expect(adapter.quoted_false).to eq("false")
    end
  end
end