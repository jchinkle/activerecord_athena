begin
  Ingredient.all.to_a
rescue => e
  puts "Error class: #{e.class}"
  puts "Error message: #{e.message}"
  puts "Backtrace:"
  puts e.backtrace.first(15)
end