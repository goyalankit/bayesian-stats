require 'rubygems'
require 'mongo'
require 'pry'
require 'json'

bus_h = File.read("yelp_academic_dataset_business.json").split("\n").map { |line|  JSON.parse(line)}


# hardcording result to save time
top_10_categories_with_numbers = [["Restaurants", 21892], ["Shopping", 8919], ["Food", 7862], ["Beauty & Spas", 4738], ["Bars", 3628], ["Health & Medical", 3213], ["Automotive", 2965], ["Home Services", 2853], ["Fashion", 2566]]

# uncomment to get top_10_categores at run time
# top_10_categories_with_numbers = bus_h.collect{|i| i["categories"]}.flatten.inject(Hash.new(0)) { |total, e| total[e] += 1 ;total}.sort_by {|_key, value| value}.reverse.take(10)

top_10_categories = top_10_categories_with_numbers.collect{|c| c[0]}

category_reviews = {}
top_10_categories.each { |c| category_reviews[c] = [] }

#File.read("yelp_academic_dataset_review.json").split("\n").each do |line|
bus_h.each do |parsed_line|
  if parsed_line["categories"].inject(false){|bo,i| bo ||= top_10_categories.include?(i)}
    found_c = parsed_line["categories"] & top_10_categories

    if found_c && category_reviews[found_c.first].length < 1000
      unless category_reviews[found_c.first].include?(parsed_line["business_id"])
        category_reviews[found_c.first] << parsed_line["business_id"]
      end
    end
  end
end


@client = Mongo::Client.new([ '127.0.0.1:27017' ], :database => 'test')
@yelp = @client.database.collection("yelp")

rev_file = File.open("reviews", 'a')
categories_file = File.open("categoriesfile", 'a')

category_reviews.map do |category, list_of_res|
  list_of_res.each do |bus_id|
    buss = @yelp.find("business_id" => bus_id).first
    if buss.nil? || buss['text'].nil?
      next
    end
    text = buss['text'].tr("\n","")
    rev_file.write("#{text}\n")
    categories_file.write("#{category}\n")
  end
end

rev_file.close
categories_file.close
