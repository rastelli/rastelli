require 'spec_helper'

module Rastelli
	class Consumer
		describe Registry do

			let(:registry) { Registry.new }
			let(:streams) { ['0', '1', '2'] }

			describe '#register' do

				context 'given stream is not registered' do

					it 'registers a new stream' do
						registry.register('0')						
						registry.registered_streams.should eq(['0'])
					end
				end

				context 'given stream is registered' do

					it 'does not re-register stream' do
						registry.register('0')
						registry.register('0')

						registry.registered_streams.should eq(['0'])
					end
				end

				context 'given stream is suspended' do

				end
			end

			describe '#unregister' do

				context 'given stream that exist in registry' do

					it 'unregisters stream' do
						registry.register('0')
						registry.unregister('0')

						registry.registered_streams.should_not include('0')
						registry.registered_streams.should be_empty
					end
				end

				context 'given stream that does not exist in registry' do

					it 'is a no-op' do
						registry.register('0')
						registry.register('1')
						registry.unregister('2')

						registry.registered_streams.should match_array(['0', '1'])
					end
				end
			end

			describe '#suspend' do

				context 'given stream is registered' do

					it 'suspends the stream' do
						registry.register('0')
						registry.suspend('0')

						registry.registered_streams.should be_empty
						registry.suspended_streams.should eq(['0'])
					end
				end

				context 'given stream is not registered' do

					it 'is a no-op' do
						registry.register('0')
						registry.register('1')						
						registry.suspend('2')

						registry.registered_streams.should match_array(['0', '1'])
						registry.suspended_streams.should_not include('2')
						registry.suspended_streams.should be_empty
					end
				end
			end

			describe '#forget' do

				context 'given stream is registered' do

					it 'forgets stream' do
						registry.register('0')
						registry.forget('0')

						registry.registered_streams.should be_empty
						registry.suspended_streams.should be_empty
					end
				end

				context 'given stream is suspended' do

					it 'forgets stream' do
						registry.register('0')
						registry.suspend('0')

						registry.forget('0')

						registry.registered_streams.should be_empty
						registry.suspended_streams.should be_empty
					end
				end				
			end

			describe '#registered_streams' do

				it 'returns current streams' do
					streams.each { |stream| registry.register(stream) }

					registry.registered_streams.should match_array(streams)
				end
			end

			describe '#update_offset' do

				context 'if stream is registered' do

					it 'updates offset for given stream' do
						registry.register('0')

						registry.offset_for('0').should eq(Registry::EARLIEST_OFFSET)

						registry.update_offset('0', 42)
						registry.offset_for('0').should eq(42)
					end
				end

				context 'if stream is not registered' do

					it 'is a no-op' do
						registry.update_offset('0', 42)
						registry.offset_for('0').should be_nil
					end
				end
			end

			describe '#streams_and_offsets' do

				let(:streams_offsets) do
					{
						'0' => 0,
						'1' => 1,
						'2' => 2
					}
				end

				before(:each) do
					streams_offsets.keys.each { |stream| registry.register(stream) }
					streams_offsets.each { |stream, offset| registry.update_offset(stream, offset)}
				end

				it 'returns current streams and offsets' do
					result = registry.streams_and_offsets

					result['0'].should eq(0)
					result['1'].should eq(1)
					result['2'].should eq(2)
				end

				it 'returns Heller::Consumer::EARLIEST_OFFSET as offset for new streams' do
					registry.unregister('1')
					registry.register('1')

					result = registry.streams_and_offsets

					result['0'].should eq(0)
					result['1'].should eq(Registry::EARLIEST_OFFSET)
					result['2'].should eq(2)
				end
			end
		end
	end
end
