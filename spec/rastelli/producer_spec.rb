require 'spec_helper'

module Rastelli

	describe Producer do

		let(:partitioner) do 
			Producer::Partitioner.new(:maximum => 16, :streams => 4)
		end

		before do
			Heller::Producer.stub(:new).and_return(mock(Heller::Producer))
		end

		describe '#produce' do

			context 'when sending single messages' do

				let(:producer) { Producer.new('zk hosts', partitioner) }

				it 'produces message' do
					producer.should_receive(:drain_batch).once do |stream, buffer|
						stream.should eq(8)
						buffer.to_a.should eq([{ :payload => 'test-8' }])
					end

					producer.produce(8, { :payload => 'test-8' })
				end
			end

			context 'when batching' do

				let(:options) do
					{
						:batch => {
							:size => 3
						}
					}
				end

				let(:producer) { Producer.new('zk hosts', partitioner, options) }

				let(:batched_messages) do
					3.times.map do
						{ :payload => 'test-4' }
					end
				end

				it 'produces on the third message to the same stream' do
					producer.should_receive(:drain_batch).once do |stream, buffer|
						stream.should eq(4)
						buffer.to_a.should eq(batched_messages)
					end

					producer.produce(1, { :payload => 'test-4' })
					producer.produce(2, { :payload => 'test-4' })
					producer.produce(5, { :payload => 'test-8' })
					producer.produce(13, { :payload => 'test-0' })
					producer.produce(9, { :payload => 'test-12' })
					producer.produce(8, { :payload => 'test-8' })
					producer.produce(3, { :payload => 'test-4' })
				end
			end
		end
	end
end
