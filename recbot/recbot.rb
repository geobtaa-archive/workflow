require 'json'

puts "\n\n*********************************************************"
puts "Hi! What is your JSON file named? (Exclude the extension)"
inputfile = gets
chomped = inputfile.chomp

string = File.read("./#{chomped}.json")
doc = JSON.parse(string)

records = doc.count


i = 1
while i <= records do

output = "output #{i}"
uuid = doc["#{output}"][0]["layer_slug_s"]
# km added 20180706 to automatically add to folder by collection code
# dont know what will happen if multiple codes
code = doc["#{output}"][0]["b1g_collection_sm"]

record = doc["#{output}"][0]
%x( mkdir -p #{code}/#{uuid})

File.open("#{code}/#{uuid}/geoblacklight.json","w") do |f|
  f.write(JSON.pretty_generate(record))

end

i += 1
end
