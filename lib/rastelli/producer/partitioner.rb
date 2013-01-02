module Rastelli
	class Producer
		class Partitioner

			attr_reader :circle, :hasher, :maximum

			NUMBER_OF_STREAMS = 288.freeze

			def initialize(options = {})
				@minimum = options[:minimum] || 0
				@maximum = options[:maximum]

				@streams = options[:streams] || NUMBER_OF_STREAMS
				@slice 	 = (@maximum / @streams).to_i
				
				@hasher  = options[:hasher] || ProxyHasher
				@circle  = {}

				distribute!
			end

			def streams
				@circle.keys
			end

			def number_of_streams
				@streams
			end

			def distribute!
				@streams.times do |i|
					node = @slice * i
					@circle[node] = node
				end
			end

			def get(key)
				return nil if @circle.empty? or key.nil?
				return @circle.first.last if @circle.size == 1

				hashed = @hasher.hash(key.to_s)

				return @circle[hashed] if @circle.has_key?(hashed)

		    hashed = @circle.keys.select { |k| k > hashed }.sort.first

		    return @circle.first.last if hashed.nil?
		    return @circle[hashed]
			end

			module ProxyHasher
				def self.hash(key)
					key.to_i
				end
			end

		end
	end	
end
