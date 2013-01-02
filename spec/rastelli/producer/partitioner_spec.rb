require 'spec_helper'

module Rastelli
	class Producer
	
		describe Partitioner do

			context 'with few streams and small maximum value' do

				module FakeHasher
					def self.hash(key)
						key.to_i
					end
				end				

				let(:partitioner) { Partitioner.new(maximum: 16, streams: 4, hasher: FakeHasher) }

				context 'when given key' do

					context 'that is within range' do

						let(:key) { 3 }

						it 'returns correct stream' do
							stream = partitioner.get(key)
							stream.should eq(4)
						end
					end

					context 'that equals a stream "node"' do

						let(:key) { 4 }

						it 'returns correct stream' do
							stream = partitioner.get(key)
							stream.should eq(4)
						end
					end

					context 'that is greater than maximum' do

						let(:key) { 17 }

						it 'returns first vnode' do
							vnode = partitioner.get(key)
							vnode.should eq(0)
						end
					end

					context 'that is empty' do

						let(:key) { nil }

						it 'returns nil' do
							vnode = partitioner.get(key)
							vnode.should be_nil
						end
					end
				end
			end

			context 'with a shit ton of streams' do

				module Base36Hasher
					def self.hash(key)
						key.to_i(36)
					end
				end

				let(:partitioner) { Partitioner.new(:maximum => maximum, :streams => streams, :hasher => Base36Hasher) }
				let(:maximum) { "ZZZZZZ".to_i(36) }
				let(:streams) { 288 }
				let(:slice) { maximum / streams }

				context 'when given key' do

					context 'that is within range' do

						let(:keys) { (0..maximum).step(slice) }

						it 'should return correct stream' do
							partitioner.streams.each do |partitioner_key|
								vnode = partitioner.get(keys.next.to_s(36).upcase)
								vnode.should eq(partitioner_key)
							end
						end
					end

					context 'that is greater than maximum' do

						let(:key) { maximum.to_s(36) }

						it 'should return first vnode' do
							vnode = partitioner.get(key)
							vnode.should eq(0)
						end
					end

					context 'that is nil' do

						let(:key) { nil }

						it 'should return nil' do
							vnode = partitioner.get(key)
							vnode.should be_nil
						end
					end
				end
			end
		end
	end
end
