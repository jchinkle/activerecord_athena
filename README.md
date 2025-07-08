# ActiveRecord Athena Adapter

An ActiveRecord adapter for AWS Athena that enables Rails applications to connect to and query AWS Athena.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'activerecord_athena'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install activerecord_athena

## Usage

### Configuration

Add the following to your `database.yml`:

```yaml
development:
  adapter: athena
  database: your_athena_database
  s3_output_location: s3://your-bucket/query-results/
  work_group: primary
  aws_config:
    region: us-east-1
    access_key_id: <%= ENV['AWS_ACCESS_KEY_ID'] %>
    secret_access_key: <%= ENV['AWS_SECRET_ACCESS_KEY'] %>
```

### Basic Usage

```ruby
# Define a model
class LogEntry < ActiveRecord::Base
  self.table_name = "log_entries"
end

# Query data
LogEntry.where("timestamp > ?", 1.day.ago).limit(100)

# Raw SQL queries
ActiveRecord::Base.connection.execute("SELECT * FROM log_entries LIMIT 10")
```

### Limitations

Due to the nature of AWS Athena, this adapter has several limitations:

- **No migrations**: Athena doesn't support traditional CREATE TABLE statements. Tables are typically created as external tables pointing to S3 data.
- **No primary keys**: Athena doesn't support primary key constraints.
- **No foreign keys**: Athena doesn't support foreign key constraints.
- **No indexes**: Athena doesn't support traditional indexes.
- **Read-only operations**: This adapter is primarily designed for querying data, not for write operations.

### Supported Operations

- `SELECT` queries with `WHERE`, `ORDER BY`, `LIMIT`, etc.
- `SHOW TABLES` - list all tables in the database
- `DESCRIBE table` - get table schema information
- `DROP TABLE` - remove external tables

### AWS Configuration

The adapter requires proper AWS credentials and permissions. You can configure these through:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. IAM roles (recommended for EC2/ECS deployments)
3. AWS credentials file
4. Direct configuration in `database.yml`

Required permissions:
- `athena:StartQueryExecution`
- `athena:GetQueryExecution`
- `athena:GetQueryResults`
- `s3:GetObject`
- `s3:ListBucket`
- `s3:PutObject` (for query results)

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests.

To install this gem onto your local machine, run `bundle exec rake install`.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/yourusername/activerecord_athena.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).