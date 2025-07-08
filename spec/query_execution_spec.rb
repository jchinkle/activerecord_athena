require "spec_helper"

RSpec.describe "Query Execution" do
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

  describe "#substitute_binds" do
    it "returns original SQL when no binds provided" do
      sql = "SELECT * FROM users"
      result = adapter.send(:substitute_binds, sql, [])
      expect(result).to eq(sql)
    end

    it "replaces ? placeholders with quoted values" do
      sql = "SELECT * FROM users WHERE id = ? AND name = ?"
      binds = [
        double(value: 123),
        double(value: "John")
      ]
      
      result = adapter.send(:substitute_binds, sql, binds)
      expect(result).to eq("SELECT * FROM users WHERE id = 123 AND name = 'John'")
    end

    it "handles binds without value method" do
      sql = "SELECT * FROM users WHERE id = ?"
      binds = [123]
      
      result = adapter.send(:substitute_binds, sql, binds)
      expect(result).to eq("SELECT * FROM users WHERE id = 123")
    end

    it "leaves extra ? placeholders unchanged when not enough binds" do
      sql = "SELECT * FROM users WHERE id = ? AND name = ?"
      binds = [123]
      
      result = adapter.send(:substitute_binds, sql, binds)
      expect(result).to eq("SELECT * FROM users WHERE id = 123 AND name = ?")
    end
  end

  describe "#quote" do
    it "quotes strings correctly" do
      expect(adapter.send(:quote, "hello")).to eq("'hello'")
      expect(adapter.send(:quote, "it's")).to eq("'it''s'")
    end

    it "handles integers and floats" do
      expect(adapter.send(:quote, 123)).to eq("123")
      expect(adapter.send(:quote, 12.34)).to eq("12.34")
    end

    it "handles booleans" do
      expect(adapter.send(:quote, true)).to eq("true")
      expect(adapter.send(:quote, false)).to eq("false")
    end

    it "handles nil" do
      expect(adapter.send(:quote, nil)).to eq("NULL")
    end

    it "handles dates and times" do
      date = Date.new(2023, 12, 25)
      expect(adapter.send(:quote, date)).to eq("'2023-12-25'")
      
      time = Time.new(2023, 12, 25, 10, 30, 45)
      expect(adapter.send(:quote, time)).to eq("'2023-12-25 10:30:45'")
    end

    it "handles other objects by converting to string" do
      expect(adapter.send(:quote, :symbol)).to eq("'symbol'")
    end
  end

  describe "#to_sql" do
    it "returns SQL string for string input" do
      sql = "SELECT * FROM users"
      expect(adapter.send(:to_sql, sql, [])).to eq(sql)
    end

    it "calls to_sql on objects that respond to it" do
      arel_object = double(to_sql: "SELECT * FROM users")
      expect(adapter.send(:to_sql, arel_object, [])).to eq("SELECT * FROM users")
    end
  end

  describe "client initialization" do
    it "lazy loads athena client" do
      expect(Aws::Athena::Client).to receive(:new).with(config[:connection_options][:aws_config]).and_return(double)
      adapter.send(:athena_client)
    end

    it "lazy loads s3 client" do
      expect(Aws::S3::Client).to receive(:new).with(config[:connection_options][:aws_config]).and_return(double)
      adapter.send(:s3_client)
    end

    it "uses empty hash when no aws_config provided" do
      config_without_aws = config.dup
      config_without_aws[:connection_options] = {}
      adapter_without_aws = ActiveRecord::ConnectionAdapters::AthenaAdapter.new(config_without_aws)
      
      expect(Aws::Athena::Client).to receive(:new).with({}).and_return(double)
      adapter_without_aws.send(:athena_client)
    end
  end

  describe "#reconnect!" do
    it "resets client instances" do
      # Access clients to initialize them
      adapter.send(:athena_client)
      adapter.send(:s3_client)
      
      # Reconnect should reset them
      adapter.reconnect!
      
      # Accessing again should create new instances
      expect(Aws::Athena::Client).to receive(:new).and_return(double)
      expect(Aws::S3::Client).to receive(:new).and_return(double)
      
      adapter.send(:athena_client)
      adapter.send(:s3_client)
    end
  end
end